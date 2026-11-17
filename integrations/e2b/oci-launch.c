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
static int errfd=-1;
static void fail(const char *s) {int e=errno?errno:EINVAL;dprintf(2,"oci-launch: %s: %s\n",s,strerror(e));if(errfd>=0)write(errfd,&e,sizeof e);_exit(126);}
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
int main(int argc,char **argv){
 if(argc<9){fprintf(stderr,"usage: oci-launch start|exec ROOT STATE CWD USER ENVCOUNT ENV... -- CMD...\n");return 2;}
 int count=number(argv[6]);if(count>4096||7+count>=argc||strcmp(argv[7+count],"--")||8+count>=argc)return 2;int first=8+count;
 if(!strcmp(argv[1],"exec")){run(argv,first,count);return 126;}if(strcmp(argv[1],"start"))return 2;
 if(mkdir(argv[3],0700))fail("state already exists or unavailable");
 int dir=open(argv[3],O_DIRECTORY|O_RDONLY|O_CLOEXEC|O_NOFOLLOW);if(dir<0)fail("state directory");
 int out=openat(dir,"stdout",O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600),err=openat(dir,"stderr",O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);if(out<0||err<0)fail("logs");int report[2];if(pipe2(report,O_CLOEXEC))fail("report pipe");pid_t supervisor=fork();if(supervisor<0)fail("fork supervisor");
 if(!supervisor){close(report[0]);if(setsid()<0)fail("setsid");int null=open("/dev/null",O_RDONLY);if(null<0||dup2(null,0)<0||dup2(out,1)<0||dup2(err,2)<0)fail("stdio");close(null);close(out);close(err);int hand[2];if(pipe2(hand,O_CLOEXEC))fail("exec pipe");pid_t child=fork();if(child<0)fail("fork command");
  if(!child){close(hand[0]);close(report[1]);close(dir);errfd=hand[1];run(argv,first,count);}
  close(hand[1]);int error=0;struct pollfd pollfd={.fd=hand[0],.events=POLLIN|POLLHUP};int pollrc=poll(&pollfd,1,10000);ssize_t n=pollrc>0?read(hand[0],&error,sizeof error):-1;close(hand[0]);if(n<0){error=ETIMEDOUT;kill(child,SIGKILL);}int answer[2]={(int)child,n==0?0:error};write(report[1],answer,sizeof answer);close(report[1]);int status;while(waitpid(child,&status,0)<0)if(errno!=EINTR)fail("wait");int code=WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status);int fd=openat(dir,"exit.tmp",O_CREAT|O_WRONLY|O_EXCL|O_CLOEXEC,0600);if(fd<0)fail("exit status");dprintf(fd,"%d\n",code);if(fsync(fd))fail("sync exit");close(fd);if(renameat(dir,"exit.tmp",dir,"exit")||fsync(dir))fail("publish exit");close(dir);_exit(0);
 }
 close(report[1]);close(out);close(err);close(dir);int answer[2];size_t got=0;while(got<sizeof answer){ssize_t n=read(report[0],(char*)answer+got,sizeof answer-got);if(n<=0)fail("startup handshake");got+=n;}close(report[0]);if(answer[1]){errno=answer[1];fail("application startup");}printf("%d\n",answer[0]);return 0;
}
