// SPDX-License-Identifier: Apache-2.0
#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/random.h>
#include <string.h>
int main(int argc, char **argv) {
    if (argc != 2) return 1;
    if (!strcmp(argv[1], "init") && access("/enable-http", F_OK) == 0) {
        pid_t child = fork();
        if (child < 0) return 11;
        if (child == 0) {
            execl("/bin/busybox", "busybox", "httpd", "-f", "-p", "8080", "-h", "/", (char *)NULL);
            _exit(12);
        }
    }
    char state[128], temporary[160], command[128], history[128], starts[128];
    snprintf(state,sizeof(state),"/%s-state",argv[1]);
    snprintf(temporary,sizeof(temporary),"%s.tmp",state);
    snprintf(command,sizeof(command),"/%s-command",argv[1]);
    snprintf(history,sizeof(history),"/%s-history",argv[1]);
    snprintf(starts,sizeof(starts),"/%s-starts",argv[1]);
    unsigned int count=0; FILE *f=fopen(starts,"r");
    if(f){if(fscanf(f,"%u",&count)!=1)return 2;fclose(f);}
    f=fopen(starts,"w");if(!f)return 3;fprintf(f,"%u\n",count+1);fclose(f);
    uint64_t nonce=0; if(getrandom(&nonce,sizeof(nonce),0)!=sizeof(nonce))return 4;
    size_t n=16*1024*1024/sizeof(uint64_t); volatile uint64_t *ram=malloc(n*sizeof(*ram));
    if(!ram)return 5;
    for(size_t i=0;i<n;i++)ram[i]=10;
    uint64_t value=10,tick=0; FILE *log=fopen(history,"a"); if(!log)return 6;
    int disk_fd=-1; uint64_t *written=NULL, *readback=NULL;
    uint64_t expected[128]={0}; unsigned char initialized[128]={0};
    for(;;){
        unsigned long v=0; f=fopen(command,"r");
        if(f){int ok=fscanf(f,"%lu",&v);fclose(f);if(ok==1 && v!=value){
            for(size_t i=0;i<n;i++)if(ram[i]!=value)return 7;
            value=v;for(size_t i=0;i<n;i++)ram[i]=value;
        }}
        if(ram[0]!=value||ram[n-1]!=value)return 8;
        // Optional real block I/O stress, inherited mid-loop across VM fork.
        // O_DIRECT prevents the page cache from hiding backing-chain mistakes.
        if (!strcmp(argv[1], "init") && access("/enable-disk-probe",F_OK)==0) {
            if(disk_fd<0) {
                disk_fd=open("/io-data",O_CREAT|O_RDWR|O_DIRECT,0600);
                if(disk_fd<0 || posix_memalign((void **)&written,4096,65536) ||
                   posix_memalign((void **)&readback,4096,65536))return 13;
            }
            unsigned int slot=tick%128;
            uint64_t pattern=(tick<<32)^value^nonce;
            for(size_t j=0;j<65536/8;j++)written[j]=pattern;
            // Cross qcow2 cluster boundaries, including inherited partial clusters.
            if(pwrite(disk_fd,written,65536,4096+(off_t)slot*65536)!=65536 || fsync(disk_fd))return 14;
            expected[slot]=pattern;initialized[slot]=1;
            unsigned int check=(slot+65)%128;
            if(initialized[check]) {
                if(pread(disk_fd,readback,65536,4096+(off_t)check*65536)!=65536)return 15;
                for(size_t j=0;j<65536/8;j++)if(readback[j]!=expected[check])return 16;
            }
        }
        f=fopen(temporary,"w");if(!f)return 9;
        fprintf(f,"%lu %lu %lu\n",(unsigned long)++tick,(unsigned long)value,(unsigned long)nonce);fclose(f);
        if(rename(temporary,state))return 10;
        fprintf(log,"%lu %lu\n",(unsigned long)tick,(unsigned long)value);fflush(log);
        usleep(50000);
    }
}
