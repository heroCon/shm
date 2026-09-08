#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>
#include "shm_pubsub.h"
#include "test_topic.h"
static std::atomic<bool> running(true);
static void stop(int){ running.store(false); }
int main(int argc,char** argv){ std::signal(SIGINT,stop); std::signal(SIGTERM,stop); ShmPubSub::Options o; if(argc>1)o.name=argv[1]; try { ShmPubSub pub(ShmPubSub::PUBLISHER,o); TestTopic msg{}; while(running.load()){ msg.timestamp++; auto r=pub.publish_result(&msg,sizeof(msg)); if(!r.ok()) std::cerr<<"publish status="<<static_cast<int>(r.status)<<'\n'; std::this_thread::sleep_for(std::chrono::milliseconds(1)); } pub.stop(); } catch(const std::exception& e){ std::cerr<<e.what()<<'\n'; return 1; } }
