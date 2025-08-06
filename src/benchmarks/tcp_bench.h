#pragma once

#include <string>



void run_tcp_server(unsigned short port);



void run_tcp_client(const std::string& host, unsigned short port, int num_pings, const std::string& payload_str);