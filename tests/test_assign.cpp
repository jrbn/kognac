#include <string>
#include <mutex>
#include <condition_variable>

using namespace std;

bool cond = false;

bool isOk() {
    return cond;
}

int main(int argc, const char** argv) {
    mutex mut;
    std::unique_lock<std::mutex> l(mut);
    std::condition_variable cond_var;
    
    cond_var.wait(l, isOk);


}
