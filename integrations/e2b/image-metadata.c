/* SPDX-License-Identifier: Apache-2.0
 * Bounded image-tree preparation: metadata plus optional uniform regular-file
 * caching under a byte budget. No symlink is followed and no application runs.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static unsigned long entries, directories, skipped, errors;
static int limited;
static unsigned long long read_limit, bytes_read, files_read;
static struct timespec began;
static dev_t root_device;
static double elapsed_ms(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec-began.tv_sec)*1000.0 + (now.tv_nsec-began.tv_nsec)/1000000.0;
}
/* Every regular file uses the same bounded policy. Never follow a swapped
 * symlink or block on a FIFO; avoid changing access timestamps. */
static void cache_file(int parent, const char *name) {
    if (bytes_read>=read_limit) return;
    int fd=openat(parent,name,O_RDONLY|O_NOFOLLOW|O_CLOEXEC|O_NONBLOCK|O_NOATIME);
    if (fd<0) { errors++; return; }
    struct stat st;
    if (fstat(fd,&st) || !S_ISREG(st.st_mode) || st.st_dev!=root_device) {
        close(fd); skipped++; return;
    }
    char buffer[65536];
    files_read++;
    while (bytes_read<read_limit && elapsed_ms()<15000) {
        unsigned long long remain=read_limit-bytes_read;
        size_t size=remain<sizeof(buffer)?(size_t)remain:sizeof(buffer);
        ssize_t n=read(fd,buffer,size);
        if (n<0) { if (errno==EINTR) continue; errors++; break; }
        if (!n) break;
        bytes_read+=(unsigned long long)n;
    }
    close(fd);
}
/* Takes ownership of fd, including all early exits. */
static void walk(int fd, unsigned depth) {
    DIR *dir=fdopendir(fd);
    if (!dir) { close(fd); errors++; return; }
    directories++;
    struct dirent *entry;
    while ((entry=readdir(dir))) {
        if (!strcmp(entry->d_name,".") || !strcmp(entry->d_name,"..")) continue;
        if (entries>=200000 || (!(entries%128) && elapsed_ms()>(read_limit?15000:5000))) {
            limited=1; break;
        }
        entries++;
        struct stat st;
        if (fstatat(dirfd(dir),entry->d_name,&st,AT_SYMLINK_NOFOLLOW)) {
            errors++; continue;
        }
        if (st.st_dev!=root_device) { skipped++; continue; }
        if (S_ISREG(st.st_mode) && read_limit) cache_file(dirfd(dir),entry->d_name);
        if (!S_ISDIR(st.st_mode)) continue;
        if (depth>=64) { skipped++; continue; }
        int child=openat(dirfd(dir),entry->d_name,O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
        if (child<0) { errors++; continue; }
        /* Recheck after open: a concurrent mount/rename must not escape. */
        struct stat opened;
        if (fstat(child,&opened) || opened.st_dev!=root_device) {
            close(child); skipped++; continue;
        }
        walk(child,depth+1);
        if (limited) break;
    }
    closedir(dir);
}
int main(int argc,char **argv) {
    if (argc!=2 && argc!=3) return 2;
    if (argc==3) {
        char *end; errno=0; read_limit=strtoull(argv[2],&end,10);
        if (errno || *end || !*argv[2] || argv[2][0]=='-' || read_limit>536870912ULL) return 2;
    }
    clock_gettime(CLOCK_MONOTONIC,&began);
    int fd=open(argv[1],O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC);
    struct stat st;
    if (fd<0 || fstat(fd,&st)) { perror("image-metadata root"); return 1; }
    root_device=st.st_dev;
    walk(fd,0);
    printf("{\"entries\":%lu,\"directories\":%lu,\"skipped\":%lu,\"errors\":%lu,\"limited\":%s,\"elapsed_ms\":%.3f,\"regular_file_bytes_read\":%llu,\"files_read\":%llu,\"read_limit_bytes\":%llu,\"application_executed\":false}\n",
           entries,directories,skipped,errors,limited?"true":"false",elapsed_ms(),bytes_read,files_read,read_limit);
    return 0;
}
