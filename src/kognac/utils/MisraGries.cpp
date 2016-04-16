/*
 * Copyright 2016 Sourav Dutta
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/

// ----------------------------------
// Implementation of the Misra-Gries
// algorithm (MisraGries.h)
//  :sdutta
// ----------------------------------

#include <kognac/MisraGries.h>
#include <cstring>
#include <algorithm>
#include <vector>

#include <boost/log/trivial.hpp>


MG :: MG (const unsigned long size) : heapSize(size) {
    lookupMap.set_empty_key(make_pair(0, MG_EMPTY_KEY));
    lookupMap.set_deleted_key(make_pair(0, MG_DELETED_KEY));
    stringPool = new char[STR_POOL_EL_SIZE * size];
    counterVector = new std::pair<long, HashMapKey>[size];
}

void MG::addElem(const char *key, unsigned int length) {

    //Copy the element in the string pool
    memcpy(stringPool + STR_POOL_EL_SIZE * idxCounterVector.size(), key, length);
    const char *ptrStr = stringPool + STR_POOL_EL_SIZE * idxCounterVector.size();

    //Add a new entry in the countersVector
    const size_t idx = idxCounterVector.size();
    counterVector[idx] = std::make_pair(1, make_pair(length, ptrStr));
    idxCounterVector.push_back(idx);

    //Add a new entry in the hash map
    lookupMap.insert(make_pair(make_pair(length, ptrStr), &(counterVector[idx].first)));
}

void MG :: add (const char *elem, unsigned int length) {

    if (length > STR_POOL_EL_SIZE - 1) {
        throw 10; //not supported
    }

    HashMap::iterator itr = lookupMap.find(make_pair(length, elem));
    if (itr == lookupMap.end()) {
        if (lookupMap.size() < heapSize) {
            addElem(elem, length);
        } else {
            //Decrease all counters
            int posFirstElementGreaterThanOne = idxCounterVector.size();
            for (int idxVector = idxCounterVector.size() - 1;
                    idxVector >= 0 &&
                    counterVector[idxCounterVector[idxVector]].first > 0;
                    idxVector--) {
                counterVector[idxCounterVector[idxVector]].first--;
                if (counterVector[idxCounterVector[idxVector]].first > 1) {
                    posFirstElementGreaterThanOne = idxVector;
                }
            }

            //Is there space for a new addition?
            const size_t idxSmallestCount = idxCounterVector.front();
            if (counterVector[idxSmallestCount].first == 0) {
                //Remove the string from the hashmap and add a new one
                lookupMap.erase(counterVector[idxSmallestCount].second);

                //Replace the string with the new one
                counterVector[idxSmallestCount].second.first = length;
                memcpy((char*)counterVector[idxSmallestCount].second.second, elem, length);
                //Update the counters in the countervector
                counterVector[idxSmallestCount].first = 1;

                //Add a new entry in the hash map
                lookupMap.insert(make_pair(counterVector[idxSmallestCount].second,
                                           &(counterVector[idxSmallestCount].first)));

                //Move the element just inserted to before posFirstElementGreaterThanOne
                idxCounterVector.insert(idxCounterVector.begin()
                                        + posFirstElementGreaterThanOne,
                                        idxSmallestCount);

                //Remove the first element from the array
                idxCounterVector.erase(idxCounterVector.begin());
            }
        }
    } else {
        (*(itr->second))++;
        MG_CountSorter sorter(counterVector);
        std::sort(idxCounterVector.begin(), idxCounterVector.end(), sorter);
    }
}

// Helper function to find the minimum value within a map structure
const StringToNumberMap MG :: getHeapElements (void) const {
    StringToNumberMap table;
    for (int i = 0; i < idxCounterVector.size(); ++i) {
        table.insert(make_pair(string(counterVector[i].second.second,
                                      counterVector[i].second.first),
                               counterVector[i].first));
    }
    return table;
}

std::vector<string> MG::getPositiveTerms() const {
    std::vector<string> results;
    for (int idxVector = idxCounterVector.size() - 1;
            idxVector >= 0 &&
            counterVector[idxCounterVector[idxVector]].first > 0;
            idxVector--) {
        results.push_back(string(
                              counterVector[idxCounterVector[idxVector]].second.second,
                              counterVector[idxCounterVector[idxVector]].second.first));
    }
    return results;
}

bool pairCompare( pair<string, unsigned long> i, pair<string, unsigned long> j) {
    return i.second < j.second;
}


// Merging algorithm for parallel Misra-Gries heaps into 1 heap from:
// "Finding Frequent Items in Parallel", M.Cafaro & P.Tempesta, Concurrency & Computation: Practice & Experience 2007
void MG :: merge (StringToNumberMap & heap) {
    StringToNumberMap::iterator it = heap.begin();

    StringToNumberMap my_heap = this->getHeapElements();

    // Get the minimum counter value of the current heap (heap[0])
    std::pair<string, unsigned long> min = *min_element(my_heap.begin(), my_heap.end(), pairCompare);
    unsigned min_counter = min.second;

    for (; it != heap.end(); it ++) {
        while (it->second > 0) {
            if (my_heap.find(it->first) == my_heap.end()) {
                StringToNumberMap::iterator myself = my_heap.begin();
                bool flag = false;
                unsigned long m = min_counter < (it->second) ? min_counter : it->second;

                string toRemove;
                for (; myself != my_heap.end(); myself++) {
                    if (myself->second > 0) {
                        myself->second -= m;
                    }
                    if ( (!flag) && (myself->second == 0) ) {
                        toRemove = myself->first;
                        //my_heap.erase(myself);
                        flag = true;
                        my_heap[it->first] = m;
                    }
                }

                if (flag) {
                    my_heap.erase(toRemove);
                }

                it->second -= m;
            } else {
                // Check if the min_counter needs to be updated
                if (min_counter == (my_heap.find(it->first))->second) {
                    my_heap[it->first] += it->second;
                    it->second = 0;
                    min = *min_element(my_heap.begin(), my_heap.end(), pairCompare);
                    min_counter = min.second;
                } else {
                    my_heap[it->first] += it->second;
                    it->second = 0;
                }
            }
        }
    }

    //I must convert the my_heap in the hashmap+vector structure
    lookupMap.clear();
    int i = 0;
    for (StringToNumberMap::iterator itr = my_heap.begin(); itr != my_heap.end();
            ++itr) {
        counterVector[i].first = itr->second;
        memcpy(stringPool + i * STR_POOL_EL_SIZE, itr->first.c_str(), itr->first.size());
        counterVector[i].second = make_pair(itr->first.size(), stringPool + i * STR_POOL_EL_SIZE);
        lookupMap.insert(make_pair(counterVector[i].second, &counterVector[i].first));
        i++;
    }

}
