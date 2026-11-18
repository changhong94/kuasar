/* SPDX-License-Identifier: Apache-2.0
 * Single-threaded guest-only launcher. VM teardown owns all descendants.
 * start ROOT STATE CWD USER ENVCOUNT ENV... -- COMMAND...
 * exec  ROOT -     CWD USER ENVCOUNT ENV... -- COMMAND...
 */
#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <grp.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdint.h>
#include <signal.h>
static int errfd=-1, readyfd=-1;
#define READY_SOCKET "/run/oci-launch.sock"
static int transfer(int fd,void *buf,size_t len,int writing){
 char *p=buf;while(len){ssize_t n=writing?write(fd,p,len):read(fd,p,len);
 if(n<0&&errno==EINTR)continue;
 if(n<=0)return -1;
 p+=n;len-=n;}return 0;
}
static void ready_reply(int pid,int error){
 uint32_t answer[2]={htonl((uint32_t)pid),htonl((uint32_t)error)};
 if(readyfd>=0){transfer(readyfd,answer,sizeof answer,1);close(readyfd);readyfd=-1;}
}
static void fail(const char *s) {int e=errno?errno:EINVAL;dprintf(2,"oci-launch: %s: %s\n",s,strerror(e));if(errfd>=0)write(errfd,&e,sizeof e);ready_reply(0,e);_exit(126);}
static unsigned number(const char *s){char *end;errno=0;unsigned long n=strtoul(s,&end,10);if(errno||!*s||*end||*s=='-'||n>=UINT32_MAX)fail("invalid id/count");return n;}
static int numeric(const char *s){if(!*s)return 0;for(;*s;s++)if(*s<'0'||*s>'9')return 0;return 1;}
static void identity(char *spec,uid_t *uid,gid_t *gid,char **name,char **home,gid_t *groups,int *ng){
 char *g=strchr(spec,':');if(g)*g++=0;int num=numeric(spec),found=0;*uid=num?number(spec):0;*gid=0;*name=NULL;*home=strdup("/");
 FILE *f=fopen("/etc/passwd","re");char *line=NULL;size_t cap=0;
 if(f){while(getline(&line,&cap,f)>0){char *v[7],*p=line;int n=0;while(n<7&&(v[n]=strsep(&p,":")))n++;if(n<6)continue;if((num&&number(v[2])==*uid)||(!num&&!strcmp(v[0],spec))){*uid=number(v[2]);*gid=number(v[3]);*name=strdup(v[0]);free(*home);*home=strdup(v[5]);found=1;break;}}fclose(f);}free(line);line=NULL;cap=0;
 if(!num&&!found)fail("unknown user");
 int explicit_gid=g&&*g;if(g&&!*g)fail("empty group");int need_group=explicit_gid&&!numeric(g);if(explicit_gid&&!need_group)*gid=number(g);
 f=fopen("/etc/group","re");int group_found=0;*ng=0;
 if(f){while(getline(&line,&cap,f)>0){char *save=line,*gn=strsep(&save,":");char *pw=strsep(&save,":");char *gi=strsep(&save,":");char *members=save;if(members)members[strcspn(members,"\n")]=0;if(!gn||!pw||!gi)continue;gid_t id=number(gi);if(need_group&&!strcmp(gn,g)){*gid=id;group_found=1;}if(!explicit_gid&&*name&&members){char *sv=NULL;for(char *m=strtok_r(members,",",&sv);m;m=strtok_r(NULL,",",&sv))if(!strcmp(m,*name)){if(*ng>=256)fail("too many groups");groups[(*ng)++]=id;break;}}}fclose(f);}free(line);if(need_group&&!group_found)fail("unknown group");
}
static void cwd_create(const char *path){if(*path!='/')fail("cwd must be absolute");char *p=strdup(path);for(char *q=p+1;;q++){if(*q=='/'||!*q){char c=*q;*q=0;if(mkdir(p,0755)&&errno!=EEXIST)fail("mkdir cwd");*q=c;if(!c)break;}}free(p);}
static void run(char **argv,int first,int count){
 if(chroot(argv[2])||chdir("/"))fail("chroot");
 cwd_create(argv[4]);uid_t uid;gid_t gid,groups[256];int ng;char *name,*home;identity(strdup(*argv[5]?argv[5]:"0"),&uid,&gid,&name,&home,groups,&ng);
 if(clearenv())fail("clearenv");
 for(int i=0;i<count;i++){if(!strchr(argv[7+i],'='))fail("invalid environment");if(putenv(argv[7+i]))fail("putenv");}if(!getenv("PATH"))setenv("PATH","/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",1);if(!getenv("HOME"))setenv("HOME",home,1);
 if(setgroups(ng,groups)||setgid(gid)||setuid(uid)||chdir(argv[4]))fail("user/cwd");
 umask(022);execvp(argv[first],argv+first);fail("exec");
}
static int launch(int argc,char **argv){
 if(argc<9){fprintf(stderr,"usage: oci-launch start|exec ROOT STATE CWD USER ENVCOUNT ENV... -- CMD...\n");return 2;}
 int count=number(argv[6]);if(count>4096||7+count>=argc||strcmp(argv[7+count],"--")||8+count>=argc)return 2;int first=8+count;
 if(!strcmp(argv[1],"exec")){run(argv,first,count);return 126;}if(strcmp(argv[1],"start"))return 2;
 if(mkdir(argv[3],0700))fail("state already exists or unavailable");
 int dir=open(argv[3],O_DIRECTORY|O_RDONLY|O_CLOEXEC|O_NOFOLLOW);if(dir<0)fail("state directory");
 int out=openat(dir,"stdout",O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600),err=openat(dir,"stderr",O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);if(out<0||err<0)fail("logs");int report[2];if(readyfd>=0){report[0]=-1;report[1]=readyfd;}else if(pipe2(report,O_CLOEXEC))fail("report pipe");pid_t supervisor=readyfd>=0?0:fork();if(supervisor<0)fail("fork supervisor");
 if(!supervisor){if(report[0]>=0)close(report[0]);if(readyfd<0&&setsid()<0)fail("setsid");int null=open("/dev/null",O_RDONLY);if(null<0||dup2(null,0)<0||dup2(out,1)<0||dup2(err,2)<0)fail("stdio");close(null);close(out);close(err);int hand[2];if(pipe2(hand,O_CLOEXEC))fail("exec pipe");pid_t child=fork();if(child<0)fail("fork command");
  if(!child){close(hand[0]);close(report[1]);readyfd=-1;signal(SIGPIPE,SIG_DFL);close(dir);errfd=hand[1];run(argv,first,count);}
  close(hand[1]);int error=0;struct pollfd pollfd={.fd=hand[0],.events=POLLIN|POLLHUP};int pollrc=poll(&pollfd,1,10000);ssize_t n=pollrc>0?read(hand[0],&error,sizeof error):-1;close(hand[0]);if(n<0){error=ETIMEDOUT;kill(child,SIGKILL);}int answer[2]={(int)child,n==0?0:error};if(readyfd>=0){ready_reply(answer[0],answer[1]);}else{write(report[1],answer,sizeof answer);close(report[1]);}int status;while(waitpid(child,&status,0)<0)if(errno!=EINTR)fail("wait");int code=WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status);int fd=openat(dir,"exit.tmp",O_CREAT|O_WRONLY|O_EXCL|O_CLOEXEC,0600);if(fd<0)fail("exit status");dprintf(fd,"%d\n",code);if(fsync(fd))fail("sync exit");close(fd);if(renameat(dir,"exit.tmp",dir,"exit")||fsync(dir))fail("publish exit");close(dir);_exit(0);
 }
 close(report[1]);close(out);close(err);close(dir);int answer[2];size_t got=0;while(got<sizeof answer){ssize_t n=read(report[0],(char*)answer+got,sizeof answer-got);if(n<=0)fail("startup handshake");got+=n;}close(report[0]);if(answer[1]){errno=answer[1];fail("application startup");}printf("%d\n",answer[0]);return 0;
}

/* An idle, image-independent worker is captured with the VM template. It
 * accepts exactly one start request after the agent's execution gate opens.
 * No application path is accessed until that request arrives. */
static char **request_args(int fd,int *argc){
 uint32_t bytes;if(transfer(fd,&bytes,sizeof bytes,0))fail("request header");
 size_t size=ntohl(bytes);if(!size||size>16*1024*1024){errno=E2BIG;fail("request size");}
 char *data=malloc(size);if(!data)fail("request memory");
 if(transfer(fd,data,size,0)||data[size-1])fail("request payload");
 size_t count=0;for(size_t i=0;i<size;i++)if(!data[i])count++;
 if(count<9||count>16384){errno=E2BIG;fail("argument count");}
 char **argv=calloc(count+1,sizeof(char*));if(!argv)fail("argument memory");
 size_t n=0;argv[n++]=data;for(size_t i=0;i<size-1;i++)if(!data[i])argv[n++]=data+i+1;
 if(strcmp(argv[0],"/usr/bin/oci-launch")||strcmp(argv[1],"start")||
    strcmp(argv[2],"/run/image-root")||strcmp(argv[3],"/run/oci-entrypoint")){
  errno=EINVAL;fail("prepared launch contract");
 }
 *argc=(int)count;return argv;
}
static int prepare(void){
 int listener=socket(AF_UNIX,SOCK_STREAM|SOCK_CLOEXEC,0);if(listener<0)fail("socket");
 struct sockaddr_un address={.sun_family=AF_UNIX};strcpy(address.sun_path,READY_SOCKET);
 if(bind(listener,(struct sockaddr*)&address,sizeof address)||chmod(READY_SOCKET,0600)||listen(listener,1))fail("listen");
 if(mkdir("/run/oci-entrypoint",0700))fail("state already exists or unavailable");
 int dir=open("/run/oci-entrypoint",O_DIRECTORY|O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
 if(dir<0)fail("state directory");
 int out=openat(dir,"stdout",O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);
 int err=openat(dir,"stderr",O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);
 if(out<0||err<0)fail("logs");
 int prepared[2];if(pipe2(prepared,O_CLOEXEC))fail("prepare pipe");
 pid_t daemon=fork();if(daemon<0)fail("fork worker");
 if(daemon){
  close(prepared[1]);close(listener);close(out);close(err);close(dir);
  char ack;if(transfer(prepared[0],&ack,1,0)||ack!=1)fail("worker preparation");
  close(prepared[0]);printf("%d\n",(int)daemon);return 0;
 }
 close(prepared[0]);
 if(setsid()<0)fail("worker setsid");
 int null=open("/dev/null",O_RDWR);if(null<0)fail("worker stdio");
 if(dup2(null,0)<0||dup2(out,1)<0||dup2(err,2)<0)fail("worker stdio");
 if(null>2)close(null);
 close(out);close(err);signal(SIGPIPE,SIG_IGN);
 int input[2],hand[2];if(pipe2(input,O_CLOEXEC)||pipe2(hand,O_CLOEXEC))fail("worker pipes");
 pid_t child=fork();if(child<0)fail("fork command");
 if(!child){
  close(listener);close(dir);close(prepared[1]);close(input[1]);close(hand[0]);
  errfd=hand[1];signal(SIGPIPE,SIG_DFL);
  // Only the trusted launcher exists here. No chroot, image file access,
  // user selection or application exec happens before the lease request.
  int argc;char **argv=request_args(input[0],&argc);close(input[0]);
  int count=number(argv[6]);
  if(count>4096||7+count>=argc||strcmp(argv[7+count],"--")||8+count>=argc){
   errno=EINVAL;fail("launch arguments");
  }
  run(argv,8+count,count);
 }
 close(input[0]);close(hand[1]);
 char ack=1;if(transfer(prepared[1],&ack,1,1))fail("prepare acknowledgement");
 close(prepared[1]);
 readyfd=accept4(listener,NULL,NULL,SOCK_CLOEXEC);close(listener);
 if(readyfd<0)fail("accept");
 if(unlink(READY_SOCKET))fail("unlink socket");
 // Validate before transferring any application instruction to the child.
 int argc;char **argv=request_args(readyfd,&argc);
 size_t size=0;for(int i=0;i<argc;i++)size+=strlen(argv[i])+1;
 uint32_t bytes=htonl((uint32_t)size);
 if(transfer(input[1],&bytes,sizeof bytes,1)||transfer(input[1],argv[0],size,1))fail("dispatch launch");
 close(input[1]);free(argv[0]);free(argv);
 int error=0;struct pollfd pollfd={.fd=hand[0],.events=POLLIN|POLLHUP};
 int pollrc;do{pollrc=poll(&pollfd,1,10000);}while(pollrc<0&&errno==EINTR);
 ssize_t n=pollrc>0?read(hand[0],&error,sizeof error):-1;close(hand[0]);
 if(n<0){error=ETIMEDOUT;kill(child,SIGKILL);}
 ready_reply((int)child,n==0?0:error);
 int status;while(waitpid(child,&status,0)<0)if(errno!=EINTR)fail("wait");
 int code=WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status);
 int fd=openat(dir,"exit.tmp",O_CREAT|O_WRONLY|O_EXCL|O_CLOEXEC,0600);
 if(fd<0)fail("exit status");
 dprintf(fd,"%d\n",code);if(fsync(fd))fail("sync exit");close(fd);
 if(renameat(dir,"exit.tmp",dir,"exit")||fsync(dir))fail("publish exit");
 close(dir);_exit(0);
}
int main(int argc,char **argv){
 if(argc==2&&!strcmp(argv[1],"prepare"))return prepare();
 return launch(argc,argv);
}
