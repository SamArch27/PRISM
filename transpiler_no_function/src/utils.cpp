#include "utils.hpp"

string vec_join(vector<string> &vec, const string &del){
    return vec.empty() ? "" : /* leave early if there are no items in the list */
            std::accumulate( /* otherwise, accumulate */
                ++vec.begin(), vec.end(), /* the range 2nd to after-last */
                *vec.begin(), /* and start accumulating with the first item */
                [&](auto &&a, auto &&b) -> auto& { a += del; a += b; return a; });
}