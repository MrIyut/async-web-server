// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, AWS_DOCUMENT_ROOT, 2);
	memcpy(conn->request_path + 2, buf + 1, len - 1);
	conn->request_path[len + 1] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	memcpy(conn->send_buffer, NOT_FOUND, strlen(NOT_FOUND));
	conn->send_len = strlen(NOT_FOUND);
	dlog(LOG_INFO, "Failed to open the file at %s\n", conn->request_path);
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	struct stat file_stats;

	fstat(conn->fd, &file_stats);
	conn->file_size = file_stats.st_size;
	conn->file_pos = 0;

	sprintf(conn->send_buffer, REQUEST_ACCEPTED CONTENT_LENGTH_HEADER CONNECTION_HEADER, conn->file_size);
	conn->send_len = strlen(conn->send_buffer);

	dlog(LOG_INFO, "Succesfully opened the file at %s, of size: %lu bytes\n", conn->request_path, conn->file_size);
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (strstr(conn->request_path, AWS_REL_STATIC_FOLDER))
		return RESOURCE_TYPE_STATIC;
	if (strstr(conn->request_path, AWS_REL_DYNAMIC_FOLDER))
		return RESOURCE_TYPE_DYNAMIC;
	return RESOURCE_TYPE_NONE;
}

struct connection *connection_create(int sockfd)
{
	struct connection *conn = calloc(1, sizeof(struct connection));

	DIE(conn == NULL, "calloc failed");
	conn->sockfd = sockfd;
	conn->fd = -1;
	conn->eventfd = -1;
	return conn;
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	int rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);

	DIE(rc < 0, "failed to remove connection handler");

	if (conn->fd > 0)
		close(conn->fd);
	if (conn->eventfd > 0) 
		close(conn->eventfd);
	

	dlog(LOG_INFO, "Succesfully closed socket connection %d, %p\n", conn->sockfd, conn);
	tcp_close_connection(conn->sockfd);
	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);
	conn = NULL;
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	int rc;

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (SSA *)&addr, &addrlen);
	DIE(sockfd < 0, "failed to accept new connection");
	dlog(LOG_INFO, "Accepted a new connection made from: %s:%d\n", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

	/* TODO: Set socket to be non-blocking. */
	/* Make sure to keep previous flags*/
	int previous_flags = fcntl(sockfd, F_GETFL, 0);
	fcntl(sockfd, F_SETFL, previous_flags | O_NONBLOCK);
	/* TODO: Instantiate new connection handler. */
	struct connection *conn = connection_create(sockfd);
	/* TODO: Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "failed to add socket to epoll");
	/* TODO: Initialize HTTP_REQUEST parser. */
	dlog(LOG_INFO, "Successfully initialized socket connection %d, %p\n", sockfd, conn);
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	conn->fd = open(conn->request_path, O_RDWR);
	if (conn->fd == -1)
		connection_prepare_send_404(conn);
	else
		connection_prepare_send_reply_header(conn);

	/*allow for out events on request file*/
	int rc = w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);

	DIE(rc < 0, "failed to update connection handler");
	return conn->fd == -1 ? STATE_SENDING_404 : STATE_SENDING_HEADER;
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0};

	int bytes_parsed = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);

	dlog(LOG_INFO, "Parsed HTTP request path (%d bytes): %s\n", bytes_parsed, conn->request_path);
	conn->res_type = connection_get_resource_type(conn);
	conn->state = connection_open_file(conn);
	return 0;
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	char addressBuffer[64];
	get_peer_address(conn->sockfd, addressBuffer, 64);
	size_t len = BUFSIZ;
	int recv_len = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, len, MSG_DONTWAIT); // BUFSIZ => len

	if (recv_len <= 0) {
		/* error in communication ( <0 ) or connection closed (0)*/
		dlog(LOG_INFO, "%s, peer: %s\n", recv_len == 0 ?
		"Receiving connection closed" : "Receiving communication error", addressBuffer);
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}
	conn->recv_len += recv_len;

	dlog(LOG_INFO, "Received %lu bytes from %s, data: | %s |\n", conn->recv_len, addressBuffer, conn->recv_buffer);
	if (strstr(conn->recv_buffer, "\r\n\r\n"))
		parse_header(conn);
}

ssize_t connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	char addressBuffer[64];

	get_peer_address(conn->sockfd, addressBuffer, 64);

	if (conn->send_len < 0) {
		dlog(LOG_INFO, "No data to send to %s\n", addressBuffer);
		return -1;
	}

	ssize_t total_sent = 0;

	while (conn->send_len > 0) {
		size_t len = conn->send_len;
		ssize_t send_len = send(conn->sockfd, conn->send_buffer, len, MSG_DONTWAIT); // conn->send_len => len

		if (send_len <= 0) {
			/* communication error ( <0 ) or connection was closed (0)*/
			dlog(LOG_INFO, "%s, peer: %s\n", send_len == 0 ?
			"Connection was closed while sending data" : "Communication error while sending data", addressBuffer);
			conn->state = STATE_CONNECTION_CLOSED;
			return -1;
		}

		conn->send_len -= send_len;
		memcpy(conn->send_buffer, conn->send_buffer + send_len, conn->send_len);
		conn->send_buffer[conn->send_len] = '\0';
		total_sent += send_len;

		dlog(LOG_INFO, "Sent %lu bytes to %s, remaining data (%lu bytes): |%s|\n",
		send_len, addressBuffer, conn->send_len, conn->send_buffer);
	}

	if (conn->send_len == 0)
		conn->state = conn->state == STATE_SENDING_404 ? STATE_404_SENT : STATE_HEADER_SENT;
	return total_sent;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	while (conn->file_size > 0) {
		size_t count = conn->file_size;
		size_t send_len = sendfile(conn->sockfd, conn->fd, NULL, count); // conn->file_size => count

		DIE(send_len < 0, "failed to send file");
		if (send_len == 0) {
			dlog(LOG_INFO, "sendfile is 0 %s\n", conn->request_path);
			break;
		}

		conn->file_size -= send_len;
		dlog(LOG_INFO, "Sent static %lu bytes of data from %s, remaining: %lu bytes\n",
		send_len, conn->request_path, conn->file_size);
	}

	if (conn->file_size == 0) {
		dlog(LOG_INFO, "Static file %s sent successfully\n", conn->request_path);
		return STATE_CONNECTION_CLOSED;
	}

	return STATE_DATA_SENT;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	conn->eventfd = eventfd(0, 0);
	conn->async_data = calloc(conn->file_size, sizeof(char));
	io_prep_pread(&conn->iocb, conn->fd, conn->async_data, conn->file_size, 0);

	conn->piocb[0] = &conn->iocb;
	io_set_eventfd(&conn->iocb, conn->eventfd);

	int rc = w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);

	DIE(rc < 0, "failed to add in notification for when file is ready to be read");

	io_setup(1, &conn->ctx);
	conn->state = STATE_ASYNC_ONGOING;
	io_submit(conn->ctx, 1, conn->piocb);
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	int rc = read(conn->eventfd, &conn->async_read_len, sizeof(size_t));

	DIE(rc <= 0, "failed to prepare file for sending");

	rc = w_epoll_update_ptr_inout(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "failed to enable write notification for async event");

	conn->state = STATE_SENDING_DATA;
}

void connection_send_dynamic_data(struct connection *conn)
{
	char addressBuffer[64];

	get_peer_address(conn->sockfd, addressBuffer, 64);

	if (conn->file_size == 0) {
		dlog(LOG_INFO, "Dynamic file %s sent successfully\n", conn->request_path);
		conn->state = STATE_DATA_SENT;
		return;
	}

	size_t len = conn->file_size;

	if (len > 1)
		len /= 2;

	ssize_t send_len = send(conn->sockfd, conn->async_data, len, MSG_DONTWAIT); // conn->file_size => len

	if (send_len <= 0) {
		/* error in communication ( <0 ) or connection closed (0)*/
		dlog(LOG_INFO, "%s, peer: %s\n", send_len == 0 ?
		"Dynamic send connection closed" : "Dynamic send communication error", addressBuffer);
		return;
	}

	conn->file_size -= send_len;
	memcpy(conn->async_data, conn->async_data + send_len, conn->file_size);
	conn->async_data[conn->file_size] = '\0';

	dlog(LOG_INFO, "Dynamically sent %lu bytes from file %s to %s, remaining data (%lu bytes)\n",
	 send_len, conn->request_path, addressBuffer, conn->file_size);
}

void connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	if (conn->eventfd == -1)
		connection_start_async_io(conn);

	/*there will be no more out events*/
	int rc = w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);

	DIE(rc < 0, "failed to update connection handler");
}

void connection_send_file(struct connection *conn)
{
	if (conn->fd == -1) {
		/*sent all data, wait for more requests*/
		int rc = w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);

		DIE(rc < 0, "failed to update connection handler");

		conn->state = STATE_INITIAL;
		return;
	}

	if (conn->res_type == RESOURCE_TYPE_NONE) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
		connection_send_dynamic(conn);

		return;
	}
	conn->state = connection_send_static(conn);
}

void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */
	switch (conn->state) {
	case STATE_INITIAL:
		conn->state = STATE_RECEIVING_DATA;
		break;
	case STATE_RECEIVING_DATA:
		receive_data(conn);
		break;
	case STATE_ASYNC_ONGOING:
		connection_complete_async_io(conn);
		break;
	default:
		dlog(LOG_INFO, "UNKNOWN INPUT STATE EVENT %d\n", conn->state);
		connection_remove(conn);
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */
	switch (conn->state) {
	case STATE_SENDING_HEADER:
		connection_send_data(conn);
		break;
	case STATE_SENDING_404:
		connection_send_data(conn);
		break;
	case STATE_SENDING_DATA:
		connection_send_dynamic_data(conn);
		break;
	case STATE_HEADER_SENT:
		connection_send_file(conn);
		break;
	case STATE_404_SENT:
		connection_send_file(conn);
		break;
	default:
		dlog(LOG_INFO, "UNKNOWN OUTPUT STATE %d\n", conn->state);
		connection_remove(conn);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	if (event & EPOLLOUT)
		handle_output(conn);
	else if (event & EPOLLIN)
		handle_input(conn);
}

int main(void)
{
	/* TODO: Initialize asynchronous operations. */
	io_setup(10, &ctx);

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "failed to init multiplexing");

	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "failed to create server socker");

	/* TODO: Add server socket to epoll object*/
	int rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "failed to add server socker to epoll object");

	/* Uncomment the following line for debugging. */
	dlog(LOG_INFO, "Server is waiting for new connections on port %d. Server fd: %d\n", AWS_LISTEN_PORT, listenfd);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		int rc_wait = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc_wait < 0, "server failed to wait");

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		if (rev.data.fd == listenfd) {
			// new connection
			if (rev.events & EPOLLIN)
				handle_new_connection();
			continue;
		}

		struct connection *conn = (struct connection *) rev.data.ptr;

		dlog(LOG_INFO, "Handling client event: %d, on fd: %d, is input event: %d, is output event: %d, state: %d\n",
		 rev.events, rev.data.fd, rev.events & EPOLLIN, rev.events & EPOLLOUT, conn->state);
		handle_client(rev.events, rev.data.ptr);
	}

	close(epollfd);
	io_destroy(ctx);
	return 0;
}
