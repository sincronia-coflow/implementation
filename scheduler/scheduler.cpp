#include <assert.h>
#include <iostream>
#include <sys/socket.h>
#include <netdb.h>
#include <vector>
#include <map>

int make_socket(int *sock) {
    int ok;
    int yes = 1;
    struct addrinfo hints, *res, *addr;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    if ((ok = getaddrinfo(NULL, "17442", &hints, &res)) != 0) {
        perror("address resolution");
        goto ret;
    }

    for (addr = res; addr != NULL; addr = addr->ai_next) {
        if (addr->ai_family == AF_INET) {
            break;
        } else {
            continue;
        }
    }

    if ((*sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol)) < 0) {
        perror("could not create socket");
        ok = *sock;
        goto ret;
    }

    if ((ok = bind(*sock, addr->ai_addr, res->ai_addrlen)) < 0) {
        perror("bind socket");
        goto ret;
    }

    if (setsockopt(*sock, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(int)) == -1) {
        perror("setsockopt");
        goto ret;
    }

    if ((ok = listen(*sock, 1)) < 0) {
        perror("listen socket");
        goto ret;
    }

  ret:
    freeaddrinfo(res);
    return ok;
}

struct outgoing {
    uint32_t data_id;
    uint32_t size;
};

/* 
 * Message format:
 * -------------------------------------------------------------------------------
 * | Job Id   | Num Send Flows | Num Recv Flows | (Data Id, Size)...| Data Id... |
 * | u32: 4B  | u32: 4B        | u32: 4B        |   (u32, u32):8B...| u32: 4B... |
 * -------------------------------------------------------------------------------
 *
 *  Caller owns malloc'ed array of coflows in fs and recvs
 */
int recv_coflow(
    int sock, 
    uint32_t *job_id, 
    uint32_t *num_send_flows, 
    struct outgoing **fs, 
    uint32_t *num_recv_flows, 
    uint32_t **recvs
) {
    int ok;
    size_t send_len, recv_len;
    uint32_t hdr[3];
    ok = recv(sock, &hdr, 2 * sizeof(uint32_t), 0);
    if (ok < 0) {
        perror("recv coflow registration header");
        return ok;
    }

    *job_id = hdr[0];
    *num_send_flows = hdr[1];
    *num_recv_flows = hdr[2];

    send_len = hdr[1] * sizeof(struct outgoing);
    *fs = (struct outgoing*) malloc(send_len);
    ok = recv(sock, *fs, send_len, 0);
    if (ok < 0) {
        perror("recv coflows");
        return ok;
    }

    recv_len = hdr[2] * sizeof(uint32_t);
    *recvs = (uint32_t*) malloc(recv_len);
    ok = recv(sock, *recvs, recv_len, 0);
    if (ok < 0) {
        perror("recv coflows");
    }

    return ok;
}

// a view of one coflow from one node
struct coflow_slice {
    int node_conn_sock;
    uint32_t job_id;
    std::vector<struct outgoing> *send;
    std::vector<uint32_t> *recv;
};

struct flow {
    struct coflow_slice *from;
    struct coflow_slice *to;
    uint32_t data_id;
    uint32_t size;
};

struct coflow {
    uint32_t job_id;
    std::map<uint32_t, struct flow> *ready_send;
    std::map<uint32_t, struct flow> *ready_recv;
    std::vector<struct flow> *flows;
};

void got_coflow_slice(
    std::map<uint32_t, struct coflow*> *partial_coflows, 
    std::vector<struct coflow*> *coflows, 
    struct coflow_slice *cfs
) {
    struct coflow *cf;
    uint32_t job_id = cfs->job_id;
    auto curr = partial_coflows->find(job_id);
    if (curr == partial_coflows->end()) {
        auto psend = new std::map<uint32_t, struct flow>();
        auto precv = new std::map<uint32_t, struct flow>();
        auto fs = new std::vector<struct flow>();
        cf = new coflow {
            job_id,
            psend,
            precv,
            fs,
        };

        auto p = partial_coflows->insert(std::pair<uint32_t, struct coflow*>(job_id, cf));
        curr = p.first;
    }

    for (auto it = cfs->send->begin(); it != cfs->send->end(); it++) {
        auto existing = curr->second->ready_recv->find(cfs->job_id);
        if (existing == curr->second->ready_recv->end()) {
            struct flow f = flow {
                cfs,
                NULL,
                it->data_id,
                it->size,
            };

            curr->second->ready_send->insert(std::pair<uint32_t, struct flow>(cfs->job_id, f));
        } else {
            // receiver side already registered
            struct flow f = existing->second;
            assert(f.data_id == it->data_id);
            f.from = cfs;
            f.size = it->size;
            curr->second->flows->push_back(f);
            curr->second->ready_recv->erase(existing);
        }
    }

    for (auto it = cfs->recv->begin(); it != cfs->recv->end(); it++) {
        auto existing = curr->second->ready_send->find(cfs->job_id);
        if (existing == curr->second->ready_send->end()) {
            struct flow f = {
                NULL,
                cfs,
                *it,
                0,
            };

            curr->second->ready_recv->insert(std::pair<uint32_t, struct flow>(cfs->job_id, f));
        } else {
            // send side already registered
            struct flow f = existing->second;
            f.to = cfs;
            curr->second->flows->push_back(f);
            curr->second->ready_recv->erase(existing);
        }
    }

    return;
}

int main(int argv, char **argc) {
    int ok;
    int sock;
    int conn_fd;
    size_t i;
    struct sockaddr_storage remote;
    socklen_t remote_size;
    uint32_t job_id;
    uint32_t num_send_flows;
    struct outgoing *fs;
    uint32_t num_recv_flows;
    uint32_t *recvs;
    std::map<uint32_t, struct coflow*> slices;
    std::vector<struct coflow*> coflows;

    if ((ok = make_socket(&sock)) < 0) {
        return ok;
    }

    remote_size = sizeof(remote);
    while (true) {
        conn_fd = accept(sock, (struct sockaddr*)&remote, &remote_size);
        if (conn_fd < 0) {
            perror("accept client connection");
            return -1;
        }

        ok = recv_coflow(conn_fd, &job_id, &num_send_flows, &fs, &num_recv_flows, &recvs);
        if (ok < 0) {
            return -1;
        }

        auto snd = new std::vector<struct outgoing>();
        for (i = 0; i < num_send_flows; i++) {
            snd->push_back(fs[i]);
        }

        auto rcv = new std::vector<uint32_t>();
        for (i = 0; i < num_send_flows; i++) {
            rcv->push_back(recvs[i]);
        }

        struct coflow_slice *cfs = new coflow_slice {
            conn_fd,
            job_id,
            snd,
            rcv,
        };

        got_coflow_slice(&slices, &coflows, cfs);
    }
}
