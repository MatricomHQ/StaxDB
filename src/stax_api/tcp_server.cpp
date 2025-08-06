#include <iostream>
#include <string>
#include <thread> 

#include <asio.hpp>
#include <asio/ts/buffer.hpp>
#include <asio/ts/internet.hpp>





void run_tcp_server(unsigned short port) {
    try {
        boost::asio::io_context io_context;
        boost::asio::ip::tcp::acceptor acceptor(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port));

        std::cout << "TCP server listening on port " << port << std::endl;

        while (true) {
            boost::asio::ip::tcp::socket socket(io_context);
            
            acceptor.accept(socket); 

            std::cout << "Accepted connection from " << socket.remote_endpoint() << std::endl;

            
            socket.set_option(boost::asio::ip::tcp::no_delay(true));

            char data[1024]; 
            boost::system::error_code error; 

            while (true) {
                size_t length = socket.read_some(boost::asio::buffer(data, sizeof(data)), error);

                if (error == boost::asio::error::eof) {
                    std::cout << "Client disconnected cleanly." << std::endl;
                    break; 
                } 
                else if (error) {
                    throw boost::system::system_error(error); 
                }

                if (length == 8 && std::string(data, length) == "SHUTDOWN") {
                    std::cout << "Received SHUTDOWN signal. Closing server." << std::endl;
                    socket.close(); 
                    return;         
                }

                boost::asio::write(socket, boost::asio::buffer(data, length), error);
                if (error) {
                    throw boost::system::system_error(error); 
                }
            }
        }
    } catch (std::exception& e) {
        std::cerr << "TCP Server Error: " << e.what() << std::endl;
    }
}