/**
 *
 * Synchronia C interface.
 * This interface exposes only one function, coflow_send(),
 * which takes the local list of flows and their corresponding coflow ids (generated externally)
 * and transfers the data accordingly.
 */

#ifndef _SYNCHRONIA_H
#define _SYNCHRONIA_H

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netdb.h>

extern "C" {

struct Addr {
    char ip[16];
    char port[6];
};

struct SendFlow {
    uint32_t data_id;
    Addr to;       // destination
    uint32_t size; // data size
    void *data;    // data

    int __sockfd;  // internal, host only: socket
};

struct RecvFlow{
    uint32_t data_id;
    Addr from;     // remote address
    void *data;    // where to place received data

    int __sockfd;  // internal, host only: socket
};

struct Coflow {
    uint32_t job_id;
    uint32_t num_send_flows;
    uint32_t num_recv_flows;
    struct SendFlow *sends;
    struct RecvFlow *recvs;
};

struct __attribute__((packed, aligned(4))) __send_flow {
    uint32_t data_id;
    uint32_t size;
};


int create_recv_flow(struct RecvFlow *f) {
    int ok;
    int sockfd;
    struct addrinfo hints, *res, *addr;
    int yes = 1;
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    if ((ok = getaddrinfo(NULL, "17442", &hints, &res)) != 0) {
        perror("address resolution");
        goto recv_flow_ret;
    }

    for (addr = res; addr != NULL; addr = addr->ai_next) {
        if (addr->ai_family == AF_INET) {
            break;
        } else {
            continue;
        }
    }

    if ((sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
        perror("could not create socket");
        ok = sockfd;
        goto recv_flow_ret;
    }

    if ((ok = bind(sockfd, addr->ai_addr, res->ai_addrlen)) < 0) {
        perror("bind socket");
        goto recv_flow_ret;
    }

    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(int)) == -1) {
        perror("setsockopt");
        goto recv_flow_ret;
    }

    if ((ok = listen(sockfd, 1)) < 0) {
        perror("listen socket");
        goto recv_flow_ret;
    }

    f->__sockfd = sockfd;

  recv_flow_ret:
    freeaddrinfo(res);
    return ok;
};

int create_send_flow(struct SendFlow *f) {
    int ok;
    int sockfd;
    struct addrinfo hints, *res, *addr;
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if ((ok = getaddrinfo(f->to.ip, f->to.port, &hints, &res)) != 0) {
        perror("address resolution");
        goto send_flow_ret;
    }

    for (addr = res; addr != NULL; addr = addr->ai_next) {
        if (addr->ai_family == AF_INET) {
            break;
        } else {
            continue;
        }
    }

    if ((sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
        perror("could not create socket");
        ok = sockfd;
        goto send_flow_ret;
    }

    f->__sockfd = sockfd;

  send_flow_ret:
    freeaddrinfo(res);
    return ok;
}

/* 
 * Message format:
 * -------------------------------------------------------------------------------
 * | Job Id   | Num Send Flows | Num Recv Flows | (Data Id, Size)...| Data Id... |
 * | u32: 4B  | u32: 4B        | u32: 4B        |   (u32, u32):8B...| u32: 4B... |
 * -------------------------------------------------------------------------------
 */
int send_to_scheduler(int scheduler_sock, struct Coflow *cf) {
    size_t i;
    int sent;
    size_t buf_len;
    struct Coflow *buf;
    struct __send_flow *curr;
    uint32_t *curr_int;
    void *to_send;
    size_t cum_sent = 0;

    buf_len = 3 * sizeof(uint32_t) + cf->num_send_flows * sizeof(__send_flow) + cf->num_recv_flows * sizeof(uint32_t);
    to_send = malloc(buf_len);
    if (to_send == NULL) {
        return -1;
    }

    buf = (struct Coflow*) to_send;
    buf->job_id = cf->job_id;
    buf->num_send_flows = cf->num_send_flows;
    buf->num_recv_flows = cf->num_recv_flows;
    curr = (struct __send_flow*) (((uint32_t*) buf) + 3 * sizeof(uint32_t));
    for (i = 0; i < cf->num_send_flows; i++) {
        curr->data_id = cf->sends[i].data_id;
        curr->size = cf->sends[i].size;
        curr += sizeof(struct __send_flow);
    }

    curr_int = (uint32_t*) curr;
    for (i = 0; i < cf->num_recv_flows; i++) {
        *curr_int = cf->recvs[i].data_id;
        curr_int += sizeof(uint32_t);
    }

    while (cum_sent < buf_len) {
        sent = send(scheduler_sock, buf, buf_len-sent, 0);
        if (sent < 0) {
            perror("send to scheduler");
            break;
        }

        buf += sent;
        cum_sent += sent;
    }

    free(to_send);

    return 0;
};

struct __attribute__((packed, aligned(4))) __coflow_priority {
    uint32_t job_id;
    uint32_t prio;
};

/* 
 * The number of local unique coflows is known from cf.
 * Message format:
 * -------------------------------------
 * | Num Coflows | (Job Id, Priority)...
 * | u32: 4B     | (u32, u32): 8B...
 * -------------------------------------
 */
int recv_from_scheduler(int scheduler_sock, int num_coflows, void *buf, size_t buf_size) {
    int ok, i;
    struct __coflow_priority *cfs, *curr;

    ok = recv(scheduler_sock, buf, buf_size, 0);
    if (ok < 0) {
        perror("recv from scheduler");
        return ok;
    }

    // check the scheduler's number of coflows is equal to ours
    if (num_coflows != *((uint32_t*) buf)) {
        fprintf(stderr, "scheduler: %d coflows; local: %d coflows\n", *((uint32_t*) buf), num_coflows);
    }

    cfs = (struct __coflow_priority*) (((uint32_t*) buf) + sizeof(uint32_t)); 
    curr = cfs;
    for (i = 0; i < num_coflows; i++) {
        printf("coflow %d: prio %d\n", curr->job_id, curr->prio);
        curr += sizeof(struct __coflow_priority);
    }

    return 0;
};

/* Opens a connection to the central synchronia scheduler.
 */
int connect_scheduler(int *sockfd, struct Addr scheduler_addr) {
    int ok;
    struct addrinfo hints, *res, *addr;
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if ((ok = getaddrinfo(scheduler_addr.ip, scheduler_addr.port, &hints, &res)) != 0) {
        perror("address resolution");
        goto sch_snd_ret;
    }

    for (addr = res; addr != NULL; addr = addr->ai_next) {
        if (addr->ai_family == AF_INET) {
            break;
        } else {
            continue;
        }
    }

    if ((*sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
        perror("could not create socket");
        ok = *sockfd;
        goto sch_snd_ret;
    }

    if ((ok = connect(*sockfd, res->ai_addr, res->ai_addrlen)) < 0) {
        perror("could not create socket");
        goto sch_snd_ret;
    }

  sch_snd_ret:
    freeaddrinfo(res);
    return ok;
};

/* Contacts the central synchronia scheduler
 * with coflow metadata (id, [sending flows])
 * Immediately make sockets for all flows.
 * Immediately start listening for receiving flows.
 * Returns quickly (does not block waiting for scheduler)
 *
 * scheduler_sock: socket connected to scheduler
 * cf: a single coflow to register
 */
int register_coflow(int scheduler_sock, struct Coflow *cf) {
    int ok, i;

    ok = send_to_scheduler(scheduler_sock, cf);
    if (ok < 0) {
        perror("send coflow to scheduler");
        return ok;
    }

    // create sockets
    for (i = 0; i < cf->num_recv_flows; i++) {
        create_recv_flow(&cf->recvs[i]);
    }

    for (i = 0; i < cf->num_send_flows; i++) {
        create_send_flow(&cf->sends[i]);
    }

    return 0;
};

/* Called once all coflows have been registered.
 * Blocks and waits for priorities from scheduler, then 
 * sends all outgoing coflows using the given priorities.
 *
 * scheduler_sock: socket connected to scheduler
 * cfs: array of local coflows
 * num_coflows: length of cfs array
 */
int send_coflows(int scheduler_sock, struct Coflow *cfs, int num_coflows) {
    int ok;
    size_t buf_size;
    void *buf;

    // get coflow priorities
    buf_size = sizeof(uint32_t) + num_coflows * sizeof(struct __coflow_priority);
    buf = malloc(buf_size);
    if (buf == NULL) {
        return -1;
    }

    ok = recv_from_scheduler(scheduler_sock, num_coflows, buf, buf_size);

    // prioritize and send the flows somehow, based on received ordering...
    return ok;
};

}
#endif
