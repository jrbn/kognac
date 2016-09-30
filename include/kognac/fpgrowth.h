/*
 * Copyright 2016 Jacopo Urbani
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

#ifndef _FPGROWTH_H
#define _FPGROWTH_H

#include <map>
#include <vector>
#include <assert.h>
#include <utility>
#include <iostream>
#include <map>

using namespace std;

template<class NodeType>
struct FPattern {
    std::vector<NodeType> patternElements;
    long support;
};

template<class NodeType>
class PatternContainer {
protected:
    const int minLen;
public:
    PatternContainer(int minLen) : minLen(minLen) {
    }

    virtual void add(const FPattern<NodeType> &) = 0;
};

template<class Node, class NodeType>
class FPNodeFactory {
private:
    std::vector<std::vector<Node> > buffers;
    Node *availNode;
    Node *limit;

    const int nelements;
public:
    FPNodeFactory() : availNode(NULL), limit(NULL), nelements(10000)  {
    }

    FPNodeFactory(const int nelements) :
        availNode(NULL), limit(NULL), nelements(nelements) {
    }

    Node *get(NodeType el) {
        Node *node = NULL;
        if (availNode == limit) {
            //Create a new buffer of nodes
            buffers.push_back(std::vector<Node>(nelements));
            availNode = &(buffers.back()[0]);
            limit = &(buffers.back()[buffers.back().size() - 1]);
        }
        node = availNode;
        availNode++;
        node->setElement(el);
        return node;
    }
};

template<class NodeType>
class FPTree {
private:

    struct FPNode {
    private:
        std::map<NodeType, FPNode*> childrenIdx;

    public:
        NodeType node;
        unsigned long freq;

        FPNode *parent;
        FPNode* nextNode;

        FPNode() : freq(0), parent(NULL), nextNode(NULL) {
        }

        FPNode(const NodeType &node) : node(node), freq(0), parent(NULL),
            nextNode(NULL) {
        }

        void addChild(FPNode* child) {
            childrenIdx.insert(make_pair(child->node, child));
        }

        void setElement(const NodeType &node) {
            this->node = node;
        }

        size_t getNChildren() const {
            return childrenIdx.size();
        }

        void clearAllChildren() {
            childrenIdx.clear();
        }

        void deattachAllChildren() {
            for (auto &el : childrenIdx) {
                assert(el.second->parent != NULL);
                el.second->parent = NULL;
            }
        }

        void removeChild(const FPNode* node) {
            auto itr = childrenIdx.find(node->node);
            childrenIdx.erase(itr);
        }

        FPNode* seekChild(const NodeType &el) const {
            if (childrenIdx.count(el)) {
                auto itr = childrenIdx.find(el);
                return itr->second;
            }
            return NULL;
        }

        ~FPNode() {
        }
    };

    FPNodeFactory<FPNode, NodeType> factory;
    std::map<NodeType, FPNode*> fptable;
    std::shared_ptr<FPNode> root;

    FPNode* insertIntern(FPNode* parent,
                         const NodeType &el,
                         int incr) {

        //Search if el is among parent's children
        FPNode* child = parent->seekChild(el);
        if (child) {
            //Increment the counter
            child->freq += incr;
            return child;
        } else {
            //Add a new element in the tree
            FPNode *newNode = factory.get(el);
            newNode->parent = parent;
            //Add a child to parent
            parent->addChild(newNode);
            newNode->freq = incr;

            //Seek in the table whether the node already exists
            if (!fptable.count(el)) {
                //Add an entry to fptable
                fptable.insert(std::make_pair(el, (FPNode*)NULL));
            }

            typename std::map<NodeType, FPNode*>::iterator
            itr = fptable.find(el);
            if (itr->second) {
                newNode->nextNode = itr->second;
                itr->second = newNode;
                //Follow the trace
                /*FPNode* nextNode = itr->second;
                  while (nextNode->nextNode) {
                  nextNode = nextNode->nextNode;
                  }
                  nextNode->nextNode = newNode;
                  assert(nextNode->nextNode->nextNode == NULL);*/
            } else {
                itr->second = newNode;
            }

            return newNode;
        }
    }

    typedef std::vector<std::pair<long, std::pair<NodeType,
            FPNode*> > > SupportElements;
    void find_with_suffix(PatternContainer<NodeType> &output,
                          const FPTree &tree,
                          long minSupport,
                          const int maxLen,
                          FPattern<NodeType> &existingPattern) const {
        SupportElements input = tree.
                                getListElementsSortedBySupport(minSupport);
        for (const auto &inputPair : input) {
            assert(inputPair.first > minSupport);

            bool found = false;
            for (auto &el : existingPattern.patternElements) {
                if (el == inputPair.second.first) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                FPattern<NodeType> newPattern = existingPattern;
                newPattern.patternElements.push_back(inputPair.second.first);
                newPattern.support = inputPair.first;
                output.add(newPattern);

                if (newPattern.patternElements.size() < maxLen) {
                    //Get the prefix tree
                    std::vector<std::vector<FPNode*> > prefixes =
                                                       tree.getPrefixTree(
                                                           newPattern.
                                                           patternElements.back());

                    //Create a conditional tree
                    FPTree newTree = FPTree::createConditionalTreeFromPrefixes(
                                         root->node, prefixes, minSupport,
                                         inputPair.second.first);

                    //Make a recursive call
                    find_with_suffix(output, newTree, minSupport, maxLen, newPattern);
                }
            }
        }
    }

    std::vector<std::vector<FPNode*> > getPrefixTree(NodeType &el) const {
        std::vector<std::vector<FPNode*> > output;
        //Get node
        assert(fptable.count(el));
        FPNode* node = fptable.find(el)->second;
        while (node) {
            //Add the path from node to the root
            std::vector<FPNode*> path;
            FPNode *nodeInPath = node;
            while (nodeInPath && nodeInPath->parent) {
                path.push_back(nodeInPath);
                nodeInPath = nodeInPath->parent;
            }
            if (path.size() > 0 && nodeInPath == root.get())
                output.push_back(path);
            node = node->nextNode;
        }
        return output;
    }

    static FPTree createConditionalTreeFromPrefixes(const NodeType &root,
            const std::vector <
            std::vector<FPNode* > > &prefixes, long minSupport,
            const NodeType &leaf) {
        FPTree tree(root);

        //Add the prefixes
        for (auto &prefix : prefixes) {
            std::vector<NodeType> v;
            for (auto &el : prefix)
                if (el->node != leaf) {
                    v.push_back(el->node);
                }
            tree.insert(v, prefix.front()->freq);
        }

        //Remove the unfrequent nodes
        tree.pruneNodesInTreeWithNoSupport(minSupport);

        return tree;
    }

    static bool lessAscOrder(const std::pair<long, std::pair<NodeType,
                             FPNode*> > &el1,
                             const std::pair<long, std::pair<NodeType,
                             FPNode*> > &el2) {
        return el1.first > el2.first;
    }

    SupportElements getListElementsSortedBySupport(const long threshold)
    const {
        //Scroll the list from bottom to top
        SupportElements supportAllPatterns;
        for (const auto& itr : fptable) {
            //Calculate overall support for each pattern
            long support = 0;
            auto node = itr.second;
            for (; node; node = node->nextNode) {
                support += node->freq;
            }
            if (support > threshold) {
                supportAllPatterns.push_back(std::make_pair(support,
                                             std::make_pair(itr.first,
                                                     itr.second)));
            }
        }
        //Sort them by support desc order
        std::sort(supportAllPatterns.begin(), supportAllPatterns.end(),
                  &FPTree::lessAscOrder);
        return supportAllPatterns;
    }

public:

    FPTree(NodeType rootValue) : FPTree(rootValue, std::vector<NodeType>(), 10000) {
    }

    FPTree(NodeType rootValue, const int internalBufferSize) : FPTree(rootValue, std::vector<NodeType>(), internalBufferSize) {
    }

    FPTree(NodeType rootValue, const std::vector<NodeType> &possibleElements, const int internalBufferSize) : factory(internalBufferSize) {
        for (typename std::vector<NodeType>::const_iterator
                itr = possibleElements.begin();
                itr != possibleElements.end(); ++itr) {
            fptable.insert(make_pair(*itr, (FPNode*)NULL));
        }
        root = std::shared_ptr<FPNode>(new FPNode(rootValue));
    }

    void insert(const std::vector<NodeType> &listNodes) {
        insert(listNodes, 1);
    }

    void insert(const std::vector<NodeType> &listNodes, int incr) {
        //Insert the list in the tree
        FPNode* parent = root.get();
        for (typename std::vector<NodeType>::const_iterator
                itr = listNodes.begin();
                itr != listNodes.end(); ++itr) {
            parent = insertIntern(parent, *itr, incr);
        }
    }

    std::map<unsigned long, unsigned long> getClassesSupport() {
        std::map<unsigned long, unsigned long> output;
        for (auto &el : fptable) {
            //Get the sum of the support
            long sum = 0;
            FPNode* n = el.second;
            while (n) {
                sum += n->freq;
                n = n->nextNode;
            }
            output.insert(make_pair(el.first, sum));
        }
        return output;
    }

    void getFreqPatterns(PatternContainer<NodeType> &container, const int maxLen, const long threshold)
    const {
        //Start the extraction
        //std::vector<FPattern<NodeType>> output;
        FPattern<NodeType> initPattern;
        find_with_suffix(container, *this, threshold, maxLen, initPattern);
        //return output;
    }

    void removeAllItems(const NodeType &el) {
        if (fptable.count(el)) {
            FPNode* node = fptable.find(el)->second;
            while (node) {
                if (node->getNChildren() > 0) {
                    node->deattachAllChildren();
                    node->clearAllChildren();
                }
                if (node->parent != NULL) {
                    node->parent->removeChild(node);
                }
                node = node->nextNode;
            }
            fptable.find(el)->second = NULL;
        }
    }

    void pruneNodesInTreeWithNoSupport(const long minSupport) {
        std::vector<NodeType> nodesToRemove;
        for (auto &el : fptable) {
            //Get the sum of the support
            long sum = 0;
            FPNode* n = el.second;
            while (n) {
                sum += n->freq;
                n = n->nextNode;
            }
            if (sum < minSupport) {
                nodesToRemove.push_back(el.first);
            }
        }

        for (auto &el : nodesToRemove)
            removeAllItems(el);
    }
};

#endif
