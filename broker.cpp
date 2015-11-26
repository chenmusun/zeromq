#include "zhelpers.hpp"
#include <thread>
#include <vector>
#include<algorithm>
const std::string HAS_NO_WORKER="HASNOWORKER";
const std::string CLIENT_REQUEST="CLIENTREQUEST";
const std::string WORKER_REQUEST="WORKERREQUEST";
const std::string BROKER_REPLY="BROKERREPLY";
const std::string WORKER_CHANGED="WORKERCHANGED";
const int TRY_TIMES=3;
const int EXPIRE_TIME=1000;
class Broker{
public:
    Broker(zmq::context_t& ct):frontend_(ct,ZMQ_ROUTER),backend_(ct,ZMQ_ROUTER){
        conn_name_current_index_=0;
    }
    void SetRouterMandatoryAndTime(){
        int optval=1;
        int expire_time=EXPIRE_TIME;
        frontend_.setsockopt(ZMQ_ROUTER_MANDATORY,&optval, sizeof(int));
        frontend_.setsockopt(ZMQ_SNDTIMEO,&expire_time,sizeof(int));
        frontend_.setsockopt(ZMQ_RCVTIMEO,&expire_time,sizeof(int));
        backend_.setsockopt(ZMQ_ROUTER_MANDATORY,&optval, sizeof(int));
        backend_.setsockopt(ZMQ_SNDTIMEO,&expire_time,sizeof(int));
        backend_.setsockopt(ZMQ_RCVTIMEO,&expire_time,sizeof(int));
    }

    void BindFrontEnd(const std::string& str){
        frontend_.bind(str.c_str());
    }
    void BindBackEnd(const std::string& str){
        backend_.bind(str.c_str());
    }



    void Run(){
            zmq::pollitem_t items[2]={
                // Always poll for worker activity on backend
                { (void *)backend_, 0, ZMQ_POLLIN, 0 },
                // Poll front-end only if we have available workers
                { (void *)frontend_, 0, ZMQ_POLLIN, 0}
            };
            while (1) {
                zmq::poll(&items[0], 2, -1);
                //  Handle worker activity on backend
                if (items[0].revents & ZMQ_POLLIN) {
                //  Queue worker address for LRU routing
                    std::string name=s_recv(backend_);
                    std::string req= s_recv(backend_);
                    if(req==WORKER_REQUEST){
                        // std::cout << worker_id << "\n";
                        if(s_sendmore(backend_,name)&& s_send(backend_,BROKER_REPLY)){
                           conn_name_vec_.push_back(name);
                        }
                    }
                }
                if (items[1].revents & ZMQ_POLLIN) {
                    //  Now get next client request, route to LRU worker
                    //  Client request is [address][empty][request]
                    std::string client_addr = s_recv(frontend_);
                    // std::cout <<"client_addr is"<<client_addr << "\n";
                    std::string req = s_recv(frontend_);
                    // std::cout << "req is "<<req << "\n";
                    if(req==CLIENT_REQUEST){
                        std::string conn_name=PickOneConn();
                        if(s_sendmore(frontend_,client_addr))
                            s_send(frontend_,conn_name);
                    }
                    else {
                        std::string data=s_recv(frontend_);
                        // std::cout << "broke recvied "<<data << "\n";
                        int trytimes=0;
                        bool havesend=false;

                        while (trytimes++<TRY_TIMES){
                            if(s_sendmore(backend_,req)){
                                s_send(backend_,data);
                                if(s_sendmore(frontend_,client_addr)){
                                    // std::cout << "broke " << "\n";
                                    s_send(frontend_,BROKER_REPLY);
                                    // std::cout << "broke send client "<<client_addr<<" reply "<<BROKER_REPLY << "\n";
                                }
                                havesend=true;
                                break;
                            }
                        }

                        if(!havesend){
                            RemoveInvalidConn(req);
                            std::string conn_name=PickOneConn();
                            // while ((conn_name=PickOneConn())!=HAS_NO_WORKER) {
                            //     if(TestConn(conn_name)){
                            //         s_sendmore(frontend_,client_addr);
                            //         s_send(frontend_,conn_name);
                            //         break;
                            //     }
                            //     else{
                            //         RemoveInvalidConn(conn_name);
                            //     }
                            // }
                            // if(conn_name!=HAS_NO_WORKER){
                                s_sendmore(frontend_,client_addr);
                                if(conn_name!=HAS_NO_WORKER)
                                    s_sendmore(frontend_,WORKER_CHANGED);
                                s_send(frontend_,conn_name);
                            // }
                            std::cout << "send to backend failed" << "\n";
                        }

                    }
                }
        }
    }
private:
    std::string PickOneConn(){
        std::string conn_name=HAS_NO_WORKER;
        try{
            if(int num=conn_name_vec_.size()){
                conn_name_current_index_=conn_name_current_index_%num;
                conn_name= conn_name_vec_[conn_name_current_index_++];//worker_queue [0];
            }
        }catch(...){
            std::cout << "PickOneConn exception" << "\n";
        }

        return conn_name;
    }

    bool TestConn(const std::string& name){
        bool ret=false;
        try{
            if(s_sendmore(backend_,name)){
                s_send(backend_,"YOUALIVE");
                std::string req=s_recv(backend_);
                if(req=="ALIVE"){
                    ret=true;
                    // break;
                }

            }
        }catch(...){

        }

        return ret;
    }

    void RemoveInvalidConn(const std::string& name){
        auto pos=std::find(conn_name_vec_.begin(), conn_name_vec_.end(), name);
        if((pos-conn_name_vec_.begin())<conn_name_current_index_)
            conn_name_current_index_--;
        if(pos!=conn_name_vec_.end())
            conn_name_vec_.erase(pos);
   }
private:
    zmq::socket_t frontend_;
    zmq::socket_t backend_;
    std::vector<std::string> conn_name_vec_;
    int conn_name_current_index_;
    // int try_times;
};
int main(int argc, char *argv[])
{
    //  Prepare our context and sockets
    try{
        zmq::context_t context(1);
        Broker bro(context);
        bro.SetRouterMandatoryAndTime();
        bro.BindFrontEnd("tcp://*:5672");
        bro.BindBackEnd("tcp://*:5673");
        bro.Run();
    }catch(...){
        std::cout << "Caught an error!" << "\n";
    }

    return 0;
}
