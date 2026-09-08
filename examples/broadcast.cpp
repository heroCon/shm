#include <iostream>
#include "shm_pubsub.h"
int main(){ ShmPubSub::Options o; o.name="/shm_broadcast_example"; ShmPubSub pub(ShmPubSub::PUBLISHER,o); const char message[]="broadcast payload"; auto r=pub.publish_result(message,sizeof(message),ShmPubSub::BROADCAST); std::cout<<"delivered="<<r.delivered<<" status="<<static_cast<int>(r.status)<<'\n'; }
