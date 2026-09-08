#ifndef SHM_PUBSUB_H_
#define SHM_PUBSUB_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>

#include "lock_free_list.h"

constexpr size_t MAX_WRITERS = 8;
constexpr size_t MAX_READERS = 16;
constexpr size_t BLOCK_COUNT = 32;
constexpr size_t BLOCK_SIZE = 4096;
constexpr size_t QUEUE_CAPACITY = 32;
constexpr uint64_t HEARTBEAT_TIMEOUT = 5000;
constexpr uint64_t HEARTBEAT_INTERVAL = 1000;
constexpr uint64_t RECYCLE_INTERVAL = 2000;

struct AtomicQueue {
  struct Cell { std::atomic<size_t> sequence; size_t data; Cell() : sequence(0), data(0) {} };
  static_assert((QUEUE_CAPACITY & (QUEUE_CAPACITY - 1)) == 0, "queue capacity must be a power of two");
  Cell queue[QUEUE_CAPACITY];
  std::atomic<size_t> enqueue_pos;
  std::atomic<size_t> dequeue_pos;
  AtomicQueue() : enqueue_pos(0), dequeue_pos(0) {}
  void init() { enqueue_pos.store(0); dequeue_pos.store(0); for (size_t i=0;i<QUEUE_CAPACITY;++i) queue[i].sequence.store(i); }
  bool enqueue(size_t value) {
    size_t pos=enqueue_pos.load(std::memory_order_relaxed); Cell* cell;
    for (;;) { cell=&queue[pos&(QUEUE_CAPACITY-1)]; size_t seq=cell->sequence.load(std::memory_order_acquire);
      intptr_t d=static_cast<intptr_t>(seq)-static_cast<intptr_t>(pos);
      if (d==0) { if (enqueue_pos.compare_exchange_weak(pos,pos+1,std::memory_order_relaxed)) break; }
      else if (d<0) return false; else pos=enqueue_pos.load(std::memory_order_relaxed); }
    cell->data=value; cell->sequence.store(pos+1,std::memory_order_release); return true;
  }
  bool dequeue(size_t& value) {
    size_t pos=dequeue_pos.load(std::memory_order_relaxed); Cell* cell;
    for (;;) { cell=&queue[pos&(QUEUE_CAPACITY-1)]; size_t seq=cell->sequence.load(std::memory_order_acquire);
      intptr_t d=static_cast<intptr_t>(seq)-static_cast<intptr_t>(pos+1);
      if (d==0) { if (dequeue_pos.compare_exchange_weak(pos,pos+1,std::memory_order_relaxed)) break; }
      else if (d<0) return false; else pos=dequeue_pos.load(std::memory_order_relaxed); }
    value=cell->data; cell->sequence.store(pos+QUEUE_CAPACITY,std::memory_order_release); return true;
  }
  bool contains(size_t value) const { size_t b=dequeue_pos.load(std::memory_order_acquire), e=enqueue_pos.load(std::memory_order_acquire); for(size_t p=b;p!=e;++p) if(queue[p&(QUEUE_CAPACITY-1)].sequence.load(std::memory_order_acquire)==p+1 && queue[p&(QUEUE_CAPACITY-1)].data==value) return true; return false; }
  void clear() { size_t ignored; while (dequeue(ignored)) {} }
};

struct DataBlock { std::atomic<pid_t> owner_pid; std::atomic<size_t> ref_count; size_t data_len; char data[BLOCK_SIZE-24]; DataBlock():owner_pid(0),ref_count(0),data_len(0),data{}{} };
static_assert(sizeof(DataBlock)==BLOCK_SIZE,"unsupported DataBlock ABI");

enum SlotState : uint32_t { SLOT_FREE, SLOT_CLAIMED, SLOT_ACTIVE, SLOT_RECLAIMING };
struct ReaderInfo { std::atomic<uint32_t> state; std::atomic<uint64_t> heartbeat; std::atomic<bool> subscribed; std::atomic<uint32_t> publishers; pid_t pid; uint64_t generation; AtomicQueue broadcast_queue; ReaderInfo():state(SLOT_FREE),heartbeat(0),subscribed(false),publishers(0),pid(0),generation(0){} };
struct WriterInfo { std::atomic<uint32_t> state; std::atomic<uint64_t> heartbeat; pid_t pid; uint64_t generation; WriterInfo():state(SLOT_FREE),heartbeat(0),pid(0),generation(0){} };

struct SharedMeta {
  static constexpr uint64_t kMagic=0x53484d5055425355ULL; static constexpr uint32_t kVersion=2;
  std::atomic<uint32_t> init_state; uint64_t magic; uint32_t version; uint32_t reserved; size_t layout_size; size_t block_count; size_t block_size;
  LockFreeFreeList<uint32_t,BLOCK_COUNT> free_list; WriterInfo writers[MAX_WRITERS]; ReaderInfo readers[MAX_READERS]; AtomicQueue shared_queue;
  SharedMeta():init_state(1),magic(kMagic),version(kVersion),reserved(0),layout_size(0),block_count(BLOCK_COUNT),block_size(BLOCK_SIZE){}
};

class ShmPubSub {
 public:
  enum Role { PUBLISHER, SUBSCRIBER }; enum DeliveryMode { BROADCAST, COMPETING };
  enum class Status { OK, NO_SUBSCRIBERS, PARTIAL, QUEUE_FULL, MESSAGE_TOO_LARGE, INVALID_ARGUMENT, WRONG_ROLE, WOULD_BLOCK, BUFFER_TOO_SMALL, NOT_SUPPORTED };
  struct Result { Status status; size_t delivered; size_t required_size; bool ok() const { return status==Status::OK || status==Status::NO_SUBSCRIBERS || status==Status::PARTIAL; } };
  struct Options { std::string name; bool unlink_on_destroy; uint64_t initialization_timeout_ms; Options():name("/shm_pubsub_mq"),unlink_on_destroy(false),initialization_timeout_ms(5000){} };

  explicit ShmPubSub(Role role, const Options& options=Options()):role_(role),options_(options),shm_fd_(-1),shm_ptr_(MAP_FAILED),meta_(nullptr),blocks_(nullptr),writer_info_(nullptr),reader_info_(nullptr),running_(true) { open_region(); check_platform(); register_process(); heartbeat_thread_=std::thread(&ShmPubSub::heartbeat_loop,this); recycle_thread_=std::thread(&ShmPubSub::recycle_loop,this); }
  ~ShmPubSub(){ stop(); unregister_process(); close_region(); if(options_.unlink_on_destroy) shm_unlink(options_.name.c_str()); }
  ShmPubSub(const ShmPubSub&)=delete; ShmPubSub& operator=(const ShmPubSub&)=delete;
  void stop(){ if(!running_.exchange(false)) return; if(heartbeat_thread_.joinable()) heartbeat_thread_.join(); if(recycle_thread_.joinable()) recycle_thread_.join(); }
  static bool destroy(const std::string& name){ return shm_unlink(name.c_str())==0; }

  Result publish_result(const void* data,size_t len,DeliveryMode mode=BROADCAST){
    const size_t capacity=sizeof(blocks_[0].data); if(role_!=PUBLISHER) return {Status::WRONG_ROLE,0,0}; if(!data||len==0) return {Status::INVALID_ARGUMENT,0,0}; if(len>capacity) return {Status::MESSAGE_TOO_LARGE,0,capacity};
    size_t subscribers=get_valid_subscriber_count(); if(!subscribers) return {Status::NO_SUBSCRIBERS,0,0}; uint32_t id; if(!meta_->free_list.pop(id)) return {Status::QUEUE_FULL,0,0};
    DataBlock& b=blocks_[id]; b.owner_pid.store(getpid(),std::memory_order_release); b.ref_count.store(1,std::memory_order_release); b.data_len=len; std::memcpy(b.data,data,len); b.owner_pid.store(0,std::memory_order_release);
    size_t delivered=mode==COMPETING?publish_competing(id):publish_broadcast(id); release_ref(id);
    if(delivered==0) return {Status::QUEUE_FULL,0,0}; if(mode==BROADCAST&&delivered<subscribers) return {Status::PARTIAL,delivered,0}; return {Status::OK,delivered,0};
  }
  bool publish(const void* data,size_t len,DeliveryMode mode=BROADCAST){ return publish_result(data,len,mode).ok(); }
  Result receive_result(void* buf,size_t len,size_t& actual){ actual=0; if(role_!=SUBSCRIBER) return {Status::WRONG_ROLE,0,0}; if(!buf&&len) return {Status::INVALID_ARGUMENT,0,0}; size_t id; if(!reader_info_->broadcast_queue.dequeue(id)&&!meta_->shared_queue.dequeue(id)) return {Status::WOULD_BLOCK,0,0}; if(id>=BLOCK_COUNT) return {Status::NOT_SUPPORTED,0,0}; DataBlock& b=blocks_[id]; size_t required=b.data_len; if(len<required){ release_ref(id); actual=required; return {Status::BUFFER_TOO_SMALL,0,required}; } std::memcpy(buf,b.data,required); actual=required; release_ref(id); return {Status::OK,1,required}; }
  bool receive(void* buf,size_t len,size_t& actual){ return receive_result(buf,len,actual).status==Status::OK; }
  bool subscribe(){ if(role_!=SUBSCRIBER||!reader_info_) return false; reader_info_->subscribed.store(true,std::memory_order_release); return true; }
  bool unsubscribe(){ if(role_!=SUBSCRIBER||!reader_info_) return false; reader_info_->subscribed.store(false,std::memory_order_release); return true; }
  size_t get_valid_subscriber_count() const { size_t n=0; for(size_t i=0;i<MAX_READERS;++i) if(meta_->readers[i].state.load(std::memory_order_acquire)==SLOT_ACTIVE&&meta_->readers[i].subscribed.load(std::memory_order_acquire)) ++n; return n; }

 private:
  size_t region_size() const { return sizeof(SharedMeta)+BLOCK_COUNT*sizeof(DataBlock); }
  void open_region(){
    bool creator=false; shm_fd_=shm_open(options_.name.c_str(),O_CREAT|O_EXCL|O_RDWR,0660); if(shm_fd_>=0) creator=true; else if(errno==EEXIST) shm_fd_=shm_open(options_.name.c_str(),O_RDWR,0660); if(shm_fd_<0) throw std::runtime_error("shm_open failed");
    if(creator&&ftruncate(shm_fd_,region_size())!=0){ close(shm_fd_); shm_unlink(options_.name.c_str()); throw std::runtime_error("ftruncate failed"); }
    if(!creator){ auto deadline=now()+options_.initialization_timeout_ms; struct stat st{}; for(;;){ if(fstat(shm_fd_,&st)!=0){ close(shm_fd_); throw std::runtime_error("fstat failed"); } if(static_cast<size_t>(st.st_size)==region_size()) break; if(st.st_size!=0||now()>deadline){ close(shm_fd_); throw std::runtime_error("shared-memory layout size mismatch"); } std::this_thread::sleep_for(std::chrono::milliseconds(1)); } }
    shm_ptr_=mmap(nullptr,region_size(),PROT_READ|PROT_WRITE,MAP_SHARED,shm_fd_,0); if(shm_ptr_==MAP_FAILED){ close(shm_fd_); throw std::runtime_error("mmap failed"); }
    meta_=static_cast<SharedMeta*>(shm_ptr_); blocks_=reinterpret_cast<DataBlock*>(static_cast<char*>(shm_ptr_)+sizeof(SharedMeta));
    if(creator){ new(meta_) SharedMeta(); meta_->layout_size=region_size(); meta_->free_list.init(); meta_->shared_queue.init(); for(size_t i=0;i<MAX_READERS;++i) meta_->readers[i].broadcast_queue.init(); for(size_t i=0;i<BLOCK_COUNT;++i) new(&blocks_[i]) DataBlock(); meta_->init_state.store(2,std::memory_order_release); }
    else { auto deadline=now()+options_.initialization_timeout_ms; while(meta_->init_state.load(std::memory_order_acquire)!=2){ if(now()>deadline){ close_region(); throw std::runtime_error("shared-memory initialization timeout"); } std::this_thread::sleep_for(std::chrono::milliseconds(1)); } if(meta_->magic!=SharedMeta::kMagic||meta_->version!=SharedMeta::kVersion||meta_->layout_size!=region_size()) { close_region(); throw std::runtime_error("shared-memory version mismatch"); } }
  }
  void check_platform(){ std::atomic<size_t> a; std::atomic<uint64_t> b; std::atomic<uint32_t> c; if(!a.is_lock_free()||!b.is_lock_free()||!c.is_lock_free()||!meta_->free_list.is_lock_free()) throw std::runtime_error("required atomics are not lock-free on this platform"); }
  void register_process(){ pid_t p=getpid(); if(role_==PUBLISHER){ for(auto& w:meta_->writers){ uint32_t e=SLOT_FREE; if(w.state.compare_exchange_strong(e,SLOT_CLAIMED)){ w.pid=p; ++w.generation; w.heartbeat.store(now()); w.state.store(SLOT_ACTIVE,std::memory_order_release); writer_info_=&w; return; } } } else { for(auto& r:meta_->readers){ uint32_t e=SLOT_FREE; if(r.state.compare_exchange_strong(e,SLOT_CLAIMED)){ r.pid=p; ++r.generation; r.subscribed.store(false); r.publishers.store(0); r.broadcast_queue.init(); r.heartbeat.store(now()); r.state.store(SLOT_ACTIVE,std::memory_order_release); reader_info_=&r; return; } } } throw std::runtime_error("process slot limit reached"); }
  void unregister_process(){ if(writer_info_){ writer_info_->state.store(SLOT_FREE,std::memory_order_release); writer_info_=nullptr; } if(reader_info_){ reader_info_->subscribed.store(false,std::memory_order_release); uint32_t e=SLOT_ACTIVE; reader_info_->state.compare_exchange_strong(e,SLOT_RECLAIMING); while(reader_info_->publishers.load()) std::this_thread::yield(); drain(*reader_info_); reader_info_->state.store(SLOT_FREE,std::memory_order_release); reader_info_=nullptr; } }
  void close_region(){ if(shm_ptr_!=MAP_FAILED){ munmap(shm_ptr_,region_size()); shm_ptr_=MAP_FAILED; } if(shm_fd_>=0){ close(shm_fd_); shm_fd_=-1; } }
  size_t publish_competing(size_t id){ blocks_[id].ref_count.fetch_add(1); if(meta_->shared_queue.enqueue(id)) return 1; release_ref(id); return 0; }
  size_t publish_broadcast(size_t id){ size_t n=0; for(auto& r:meta_->readers){ if(r.state.load(std::memory_order_acquire)!=SLOT_ACTIVE||!r.subscribed.load(std::memory_order_acquire)) continue; r.publishers.fetch_add(1); if(r.state.load(std::memory_order_acquire)==SLOT_ACTIVE&&r.subscribed.load(std::memory_order_acquire)){ blocks_[id].ref_count.fetch_add(1); if(r.broadcast_queue.enqueue(id)) ++n; else release_ref(id); } r.publishers.fetch_sub(1,std::memory_order_release); } return n; }
  void release_ref(size_t id){ if(blocks_[id].ref_count.fetch_sub(1,std::memory_order_acq_rel)==1) meta_->free_list.push(static_cast<uint32_t>(id)); }
  void drain(ReaderInfo& r){ size_t id; while(r.broadcast_queue.dequeue(id)) if(id<BLOCK_COUNT) release_ref(id); }
  void heartbeat_loop(){ while(running_.load()){ auto t=now(); if(writer_info_) writer_info_->heartbeat.store(t); if(reader_info_) reader_info_->heartbeat.store(t); sleep_interruptible(HEARTBEAT_INTERVAL); } }
  void recycle_loop(){ while(running_.load()){ uint64_t t=now(); for(auto& r:meta_->readers){ uint32_t e=SLOT_ACTIVE; if(t-r.heartbeat.load()>HEARTBEAT_TIMEOUT&&r.state.compare_exchange_strong(e,SLOT_RECLAIMING)){ r.subscribed.store(false); while(r.publishers.load()) std::this_thread::yield(); drain(r); r.state.store(SLOT_FREE,std::memory_order_release); } } for(auto& w:meta_->writers){ uint32_t e=SLOT_ACTIVE; if(t-w.heartbeat.load()>HEARTBEAT_TIMEOUT) w.state.compare_exchange_strong(e,SLOT_FREE); } sleep_interruptible(RECYCLE_INTERVAL); } }
  void sleep_interruptible(uint64_t ms){ for(uint64_t n=0;n<ms&&running_.load();n+=10) std::this_thread::sleep_for(std::chrono::milliseconds(10)); }
  static uint64_t now(){ return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
  Role role_; Options options_; int shm_fd_; void* shm_ptr_; SharedMeta* meta_; DataBlock* blocks_; WriterInfo* writer_info_; ReaderInfo* reader_info_; std::thread heartbeat_thread_,recycle_thread_; std::atomic<bool> running_;
};

#endif
