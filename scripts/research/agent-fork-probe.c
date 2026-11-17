// SPDX-License-Identifier: Apache-2.0
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <linux/vm_sockets.h>
int main(int argc, char **argv) {
    (void)argv;
    if (argc > 1) {
        unsigned int cid = 0;
        int fd = open("/dev/vsock", O_RDONLY);
        if (fd < 0 || ioctl(fd, IOCTL_VM_SOCKETS_GET_LOCAL_CID, &cid)) return 2;
        printf("%u\n", cid); close(fd); return 0;
    }
    size_t n = 32 * 1024 * 1024 / sizeof(uint64_t);
    volatile uint64_t *p = malloc(n * sizeof(*p));
    if (!p) return 3;
    for (size_t i=0; i<n; i++) p[i]=10;
    FILE *f = fopen("/run/probe-ready", "w"); if (!f) return 4;
    fputs("10\n", f); fclose(f);
    unsigned int v=0;
    for (;;) {
        f=fopen("/run/probe-command", "r");
        if (f) { int r=fscanf(f,"%u",&v); fclose(f); if (r==1) break; }
        usleep(10000);
    }
    for (size_t i=0; i<n; i++) if (p[i]!=10) return 5;
    for (size_t i=0; i<n; i++) p[i]=v;
    f=fopen("/run/probe-result", "w"); if (!f) return 6;
    fprintf(f,"initial=10 value=%u\n", v); fclose(f);
    for (;;) { if (p[0]!=v) return 7; sleep(1); }
}
