#include <sys/socket.h>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
int main(int argc,char** argv){ size_t count=argc>1?std::strtoull(argv[1],nullptr,10):100000, bytes=argc>2?std::strtoull(argv[2],nullptr,10):64; int fd[2]; if(bytes==0||socketpair(AF_UNIX,SOCK_DGRAM,0,fd)) return 2; std::string data(bytes,'x'); auto start=std::chrono::steady_clock::now(); std::thread receiver([&]{ std::string out(bytes,'\0'); for(size_t i=0;i<count;++i) if(recv(fd[1],&out[0],bytes,0)<0) std::abort(); }); for(size_t i=0;i<count;++i) if(send(fd[0],data.data(),bytes,0)<0) return 3; receiver.join(); auto ns=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-start).count(); close(fd[0]);close(fd[1]); std::cout<<"transport,message_bytes,messages,seconds,msg_per_sec,dropped\nuds,"<<bytes<<','<<count<<','<<ns/1e9<<','<<count/(ns/1e9)<<",0\n"; }
