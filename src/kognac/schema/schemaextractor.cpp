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

#include <kognac/schemaextractor.h>
#include <kognac/utils.h>
#include <kognac/stringscol.h>
#include <kognac/hashfunctions.h>

#include <boost/algorithm/string/predicate.hpp>

#include <fstream>
#include <string>
#include <vector>

using namespace std;

const long SchemaExtractor::HASHCLASS = Hashes::murmur3_56(S_RDFS_CLASS,
                                        strlen(S_RDFS_CLASS));
const long SchemaExtractor::HASHTYPE = Hashes::murmur3_56(S_RDF_TYPE,
                                       strlen(S_RDF_TYPE));

#ifdef DEBUG
/*StringCollection SchemaExtractor::support(64 * 1024 * 1024);
void SchemaExtractor::initMap() {
    if (!isSet) {
        mappings.set_empty_key(-1);
        mappings.set_deleted_key(-2);
        properties.set_empty_key(-1);
        properties.set_deleted_key(-2);
        isSet = true;
    }
}
google::dense_hash_map<long, const char*> SchemaExtractor::mappings;
google::dense_hash_map<long, const char*> SchemaExtractor::properties;
bool SchemaExtractor::isSet = false;*/
#endif

bool SchemaExtractor::isPresent(const long el, vector<long> &elements) {
    for (vector<long>::iterator itr = elements.begin(); itr != elements.end();
            itr++) {
        if (el == *itr) {
            return true;
        }
    }
    return false;
}

void SchemaExtractor::addToMap(SchemaMap &map, const char *key, const char *value) {
    SchemaMap::iterator itr = map.find(key);
    if (itr == map.end()) {
        long hash = Hashes::murmur3_56(value + 2, Utils::decode_short(value));
        vector<long> newVector;
        newVector.push_back(hash);
        map.insert(make_pair(supportSubclasses.addNew(key,
                             Utils::decode_short(key) + 2), newVector));
    } else {
        long hash = Hashes::murmur3_56(value + 2, Utils::decode_short(value));
        if (!isPresent(hash, itr->second)) {
            (itr->second).push_back(hash);
        }
    }
}

void SchemaExtractor::addToMap(SchemaMap &map, const char *key, const long value) {
    SchemaMap::iterator itr = map.find(key);
    if (itr == map.end()) {
        vector<long> newVector;
        newVector.push_back(value);
        map.insert(make_pair(supportSubclasses.addNew(key, Utils::decode_short(key) + 2),
                             newVector));
    } else {
        if (!isPresent(value, itr->second)) {
            (itr->second).push_back(value);
        }
    }
}

void SchemaExtractor::addToMap(NumericNPSchemaMap &map, const long key,
                               const long value) {
    NumericNPSchemaMap::iterator itr = map.find(key);
    if (itr == map.end()) {
        vector<long> newVector;
        newVector.push_back(value);
        map.insert(make_pair(key, newVector));
    } else {
        if (!isPresent(value, itr->second)) {
            (itr->second).push_back(value);
        }
    }
}

SchemaExtractor::SchemaExtractor() : supportSubclasses(SC_SIZE_SUPPORT_BUFFER),
    supportExplicitClasses(SC_SIZE_SUPPORT_BUFFER) {
    explicitClasses.set_empty_key(NULL);
    subclasses.set_empty_key(NULL);
    outputSubclasses.set_empty_key(-1);
    domains.set_empty_key(-1);
    ranges.set_empty_key(-1);
    root = NULL;
};

void SchemaExtractor::extractSchema(char **triple) {
    int sizeP = Utils::decode_short(triple[1]);

    string pred(triple[1] + 2, sizeP);
    if (pred.compare(string(S_RDF_TYPE)) == 0) {
        if (explicitClasses.find(triple[2]) ==
                explicitClasses.end()) {
            explicitClasses.insert(supportExplicitClasses.addNew(triple[2],
                                   Utils::decode_short(triple[2]) + 2));
        }
    } else if (pred.compare(string(S_RDFS_SUBCLASS)) == 0) {
        addToMap(subclasses, triple[0], triple[2]);
        addToMap(subclasses, triple[0], HASHCLASS);
        addToMap(subclasses, triple[2], HASHCLASS);
    } else if (pred == string(S_RDFS_DOMAIN)) {
        if (explicitClasses.find(triple[2]) ==
                explicitClasses.end()) {
            explicitClasses.insert(supportExplicitClasses.addNew(triple[2],
                                   Utils::decode_short(triple[2]) + 2));
        }

        long hashS = Hashes::murmur3_56(triple[0] + 2, Utils::decode_short(triple[0]));
        if (domains.find(hashS) == domains.end()) {
            long hashO = Hashes::murmur3_56(triple[2] + 2, Utils::decode_short(triple[2]));
            domains.insert(make_pair(hashS, hashO));
        }
    } else if (pred == string(S_RDFS_RANGE)) {
        if (explicitClasses.find(triple[2]) ==
                explicitClasses.end()) {
            explicitClasses.insert(supportExplicitClasses.addNew(triple[2],
                                   Utils::decode_short(triple[2]) + 2));
        }

        long hashS = Hashes::murmur3_56(triple[0] + 2, Utils::decode_short(triple[0]));
        if (ranges.find(hashS) == ranges.end()) {
            long hashO = Hashes::murmur3_56(triple[2] + 2, Utils::decode_short(triple[2]));
            ranges.insert(make_pair(hashS, hashO));
        }
    }
};

void SchemaExtractor::merge(SchemaExtractor & schema) {
    for (SchemaMap::iterator itr = schema.subclasses.begin();
            itr != schema.subclasses.end(); ++itr) {
        for (vector<long>::iterator itr2 = itr->second.begin(); itr2 !=
                itr->second.end(); ++itr2) {
            addToMap(subclasses, itr->first, *itr2);
        }
    }

    for (TextualSet::iterator itr = schema.explicitClasses.begin();
            itr != schema.explicitClasses.end(); ++itr) {
        if (explicitClasses.find(*itr) == explicitClasses.end()) {
            explicitClasses.insert(supportExplicitClasses.addNew(*itr,
                                   Utils::decode_short(*itr) + 2));
        }
    }

    //Add the domains
    for (DomainRangeMap::iterator itr = schema.domains.begin(); itr != schema.domains.end();
            ++itr) {
        if (domains.find(itr->first) == domains.end()) {
            domains.insert(make_pair(itr->first, itr->second));
        }
    }

    for (DomainRangeMap::iterator itr = schema.ranges.begin(); itr != schema.ranges.end();
            ++itr) {
        if (ranges.find(itr->first) == ranges.end()) {
            ranges.insert(make_pair(itr->first, itr->second));
        }
    }
};

bool SchemaExtractor::isReachable(NumericSchemaMap &map, vector<long> &prevEls,
                                  long source, long dest) {
    NumericSchemaMap::iterator itr = map.find(source);
    if (itr != map.end()) {
        for (vector<long>::iterator itr2 = itr->second->begin();
                itr2 != itr->second->end(); ++itr2) {

            bool found = false;
            for (vector<long>::iterator ipe = prevEls.begin(); ipe != prevEls.end();
                    ++ipe) {
                if (*ipe == *itr2) {
                    found = true;
                    break;
                }
            }
            if (found) {
                continue;
            }

            prevEls.push_back(*itr2);
            if (*itr2 == dest || isReachable(map, prevEls, *itr2, dest)) {
                return true;
            }
            prevEls.pop_back();
        }
    }
    return false;
}

bool SchemaExtractor::isDirectSubclass(NumericSchemaMap &map, long subclass,
                                       long superclass) {
    assert(subclass != superclass);
    NumericSchemaMap::iterator itr = map.find(subclass);
    for (vector<long>::iterator itr2 = itr->second->begin(); itr2 != itr->second->end();
            ++itr2) {
        if (*itr2 == subclass || *itr2 == superclass)
            continue;

        vector<long> alreadyProcessedElements;
        alreadyProcessedElements.push_back(subclass);
        alreadyProcessedElements.push_back(*itr2);
        if (isReachable(map, alreadyProcessedElements, *itr2, superclass)) {
            return false;
        }
    }
    return true;
}

std::set<string> SchemaExtractor::getAllClasses() const {
    std::set<string> set;
    for (SchemaMap::const_iterator itr = subclasses.begin();
            itr != subclasses.end();
            ++itr) {
        string s(itr->first + 2, Utils::decode_short(itr->first));
        set.insert(s);
    }
    return set;
}

ExtNode *SchemaExtractor::buildTreeFromRoot(NumericNPSchemaMap &map,
        NumericSchemaMap &subclassMap, const long root) {
    ExtNode *node = new ExtNode(root);
    vector<ExtNode*> queueElements;
    queueElements.push_back(node);

    google::dense_hash_set<long> insertedElements;
    insertedElements.set_empty_key(-1);
    insertedElements.insert(root);

    while (queueElements.size() > 0) {
        ExtNode *node = queueElements.back();
        queueElements.pop_back();

        //Query the node and check if it has children
        NumericNPSchemaMap::iterator itr = map.find(node->key);
        if (itr != map.end()) {
            bool first = true;
            ExtNode *prevNode = NULL;
            for (vector<long>::iterator itr2 = itr->second.begin();
                    itr2 != itr->second.end(); ++itr2) {

                //Check the element was not already processed
                if (insertedElements.find(*itr2) != insertedElements.end()) {
                    continue;
                }

                //Check whether the node is a direct subclass of the parent
                if (!isDirectSubclass(subclassMap, *itr2, node->key)) {
                    continue;
                }


                insertedElements.insert(*itr2);
                ExtNode *n = new ExtNode(*itr2);
                n->parent = node;
                if (first) {
                    node->depth++;
                    ExtNode *p = node->parent;
                    int cd = node->depth;
                    while (p != NULL && p->depth == cd) {
                        p->depth++;
                        cd++;
                        p = p->parent;
                    }

                    node->child = n;
                    first = false;
                } else {
                    prevNode->sibling = n;
                }
                prevNode = n;
                queueElements.push_back(n);
            }
        }
    }
    return node;
}

void SchemaExtractor::transitiveClosure(NumericNPSchemaMap &map, ExtNode *node) {
    if (node->child != NULL) {
        transitiveClosure(map, node->child);
    }

    if (node->sibling != NULL) {
        transitiveClosure(map, node->sibling);
    }

    addToMap(map, node->key, node->assignedID);
    while (node->parent != NULL) {
        addToMap(map, node->key, node->parent->assignedID);
        node = node->parent;
    }
}

SchemaExtractor::~SchemaExtractor() {
    if (root != NULL)
        deallocateTree(root);
}

void SchemaExtractor::deallocateTree(ExtNode *node) {
    if (node->child != NULL) {
        deallocateTree(node->child);
    }
    if (node->sibling != NULL) {
        deallocateTree(node->sibling);
    }
    delete node;
}

void SchemaExtractor::assignID(ExtNode *root, long &counterID) {
    //Assign the numbers using a post-order scheme
    if (root->child != NULL) {
        assignID(root->child, counterID);
        ExtNode *sibling = root->child->sibling;
        while (sibling != NULL) {
            assignID(sibling, counterID);
            sibling = sibling->sibling;
        }
    }

#ifdef DEBUG
//    google::dense_hash_map<long, const char*>::iterator itr = mappings.find(root->key);
//    if (itr != mappings.end()) {
//        mappings.insert(make_pair(counterID, itr->second));
//        mappings.erase(itr);
//    }
#endif

    root->assignedID = counterID++;
}

void SchemaExtractor::processExplicitClasses(SchemaMap &map, TextualSet &set) {
    for (TextualSet::iterator itr = set.begin(); itr != set.end(); ++itr) {
        addToMap(map, *itr, *itr);
        addToMap(map, *itr, HASHCLASS);
    }
}

void SchemaExtractor::printTree(int padding, ExtNode* node) {
    for (int i = 0; i < padding; ++i)
        cout << "  ";
    string text = to_string(node->key);
    cout << "(" << padding << "-" << node->depth << ") " << text << "("
         << node->assignedID << ")" << endl;
    ExtNode *child = node->child;
    while (child != NULL) {
        printTree(padding + 1, child);
        child = child->sibling;
    }
}

void SchemaExtractor::processClasses(SchemaMap &map, NumericNPSchemaMap &omap) {
    //Translate the keys into numbers and build the inverse  "superclass" map
    NumericSchemaMap tmpMap;
    tmpMap.set_empty_key(-1);
    NumericNPSchemaMap inverseTmpMap;
    inverseTmpMap.set_empty_key(-1);
    for (SchemaMap::iterator itr = map.begin(); itr
            != map.end(); itr++) {
        long ks = Hashes::murmur3_56(itr->first + 2,
                                     Utils::decode_short(itr->first));
        tmpMap.insert(make_pair(ks, &(itr->second)));
        for (vector<long>::iterator itr2 = itr->second.begin();
                itr2 != itr->second.end(); ++itr2) {
            addToMap(inverseTmpMap, *itr2, ks);
        }
        //Add a mapping between the hashes and the textual strings
        hashMappings.insert(make_pair(ks, string(itr->first + 2,
                                      Utils::decode_short(itr->first))));
    }



    //Determine all roots
    google::dense_hash_set<long> roots;
    roots.set_empty_key(-1);
    for (NumericNPSchemaMap::iterator itr = inverseTmpMap.begin();
            itr != inverseTmpMap.end(); itr++) {
        NumericSchemaMap::iterator itr2 = tmpMap.find(itr->first);
        if (itr2 == tmpMap.end() || ((itr2->second)->size() == 1
                                     && (itr2->second)->at(0) == itr->first)) {
            roots.insert(itr->first);
        }
    }

    //There should be only one root: <Class>. If there are more then issue a
    //warning
    if (roots.size() > 1) {
        BOOST_LOG_TRIVIAL(error) << "There should be only one root! Found (" << roots.size() << ")";
        throw 10;
    }

    //Build the trees from the roots
    root = buildTreeFromRoot(inverseTmpMap, tmpMap, *roots.begin());

    //Rearrange tree by depth
    rearrangeTreeByDepth(root);

    //Assign a number to all the terms in the tree
    omap.clear();
    long counterID = 0;
    assignID(root, counterID);
    //printTree(0,root);

    //Compute the transitive closure and deallocate all the trees
    transitiveClosure(omap, root);

    BOOST_LOG_TRIVIAL(debug) << "Members of " << omap.size() <<
                             " classes have a clustering ID";
}

bool lessRankingPairs(pair<long, long> &p1, pair<long, long> &p2) {
    return p1.second < p2.second;
}

void SchemaExtractor::prepare() {
    //All the explicit classes become subclasses of themselves
    processExplicitClasses(subclasses, explicitClasses);

    //Perform the transitive closure
    processClasses(subclasses, outputSubclasses);

    //Sort all the properties by their ranking
    //sortPropertiesByRanking(propertyCardinality);

    //Print some stats
    //printTree(0, root);
    BOOST_LOG_TRIVIAL(debug) << "There are " << outputSubclasses.size() << " subclasses to cluster the terms";
}

void SchemaExtractor::retrieveInstances(const long term, const vector<long> **output) const {
    NumericNPSchemaMap::const_iterator itr = outputSubclasses.find(term);
    if (itr != outputSubclasses.end()) {
        *output = &(itr->second);
    } else {
        *output = NULL;
    }
};

bool SchemaExtractor::hasDomain(const long hashProperty) const {
    return domains.find(hashProperty) != domains.end();
}

long SchemaExtractor::getDomain(const long hashProperty) const {
    return domains.find(hashProperty)->second;
}

bool SchemaExtractor::hasRange(const long hashProperty) const {
    return ranges.find(hashProperty) != ranges.end();
}

long SchemaExtractor::getRange(const long hashProperty) const {
    return ranges.find(hashProperty)->second;
}

void SchemaExtractor::rearrangeTreeByDepth(ExtNode *node) {
    if (node->child != NULL && node->child->sibling != NULL) {

        vector<ExtNode*> children;
        ExtNode *child = node->child;
        while (child != NULL) {
            rearrangeTreeByDepth(child);
            children.push_back(child);
            child = child->sibling;
        }

        //Sort children
        std::sort(children.begin(), children.end(), ExtNode::less);

        //Rearrange them
        child = children[0];
        node->child = child;
        for (int i = 0; i < children.size(); ++i) {
            child->sibling = children[i];
            child = children[i];
            child->sibling = NULL;
        }
    }
}

void rearrangeChildrenWithPatterns(ExtNode *node,
                                   const std::map<unsigned long,
                                   unsigned long> &classes,
                                   const std::map < unsigned long,
                                   std::vector<unsigned long>> &neighbours) {
    if (node != NULL) {
        if (node->child != NULL) {
            if (node->child->sibling != NULL) {
                //Collect all children
                std::vector<ExtNode *> childrenToRearrange;
                ExtNode *next = node->child;
                while (next != NULL) {
                    rearrangeChildrenWithPatterns(next, classes, neighbours);
                    childrenToRearrange.push_back(next);
                    next = next->sibling;
                }

                std::vector<ExtNode*> rearrangedChildren;
                int posClass = 0;
                /*long maxSupport = classes.find(childrenToRearrange[0]->key)
                                  ->second;
                for (int i = 1; i < childrenToRearrange.size(); ++i) {
                    long key = childrenToRearrange[i]->key;
                    if (classes.find(key)->second > maxSupport) {
                        posClass = i;
                        maxSupport = classes.find(key)->second;
                    }
                }*/

                //Now I know the most popular class. Extract it.
                rearrangedChildren.push_back(childrenToRearrange[posClass]);
                childrenToRearrange.erase(childrenToRearrange.begin() +
                                          posClass);
                //Start from it to select the most relevant classes
                while (childrenToRearrange.size() > 0) {
                    //Take the last class. See if there is a friend class to use
                    long currentClass = rearrangedChildren.back()->key;
                    bool foundNext = false;
                    if (neighbours.count(currentClass)) {
                        const std::vector<unsigned long> &neighboursClasses =
                            neighbours.find(currentClass)->second;
                        //Go through the elements in the cluster. Is one still
                        //available?
                        for (auto el : neighboursClasses) {
                            int idxRemainingClass = 0;
                            for (; idxRemainingClass
                                    < childrenToRearrange.size();
                                    ++idxRemainingClass) {
                                if (childrenToRearrange[
                                            idxRemainingClass]->key == el) {
                                    foundNext = true;
                                    break;
                                }
                            }

                            if (foundNext) {
                                rearrangedChildren.push_back(
                                    childrenToRearrange[idxRemainingClass]);
                                childrenToRearrange.erase(
                                    childrenToRearrange.begin() +
                                    idxRemainingClass);
                                break;
                            }
                        }

                    }

                    //I don't know any other class.
                    //Just pick the next element
                    if (!foundNext) {
                        rearrangedChildren.push_back(
                            childrenToRearrange.front());
                        childrenToRearrange.erase(childrenToRearrange.begin());
                    }
                }

                if (childrenToRearrange.size() == 0) {
                    //I rearranged the children
                    node->child = rearrangedChildren[0];
                    ExtNode *nodeToFix = node->child;
                    for (int i = 1; i < rearrangedChildren.size(); ++i) {
                        nodeToFix->sibling = rearrangedChildren[i];
                        nodeToFix = nodeToFix->sibling;
                    }
                    nodeToFix->sibling = NULL;
                } else {
                    BOOST_LOG_TRIVIAL(debug) << "I was"
                                             "unable to rearrange a list of " <<
                                             (childrenToRearrange.size() +
                                              rearrangedChildren.size()) <<
                                             " nodes";
                }
            }
        }
    }
}

bool sorterKeyFirst(const std::pair<unsigned long, long> &el1,
                    const std::pair<unsigned long, long> &el2) {
    return el1.first < el2.first || (el1.first == el2.first && el1.second > el2.second);
}

bool sorterSupportFirst(const std::pair<unsigned long, long> &el1,
                        const std::pair<unsigned long, long> &el2) {
    return el1.second > el2.second;
}

void SchemaExtractor::rearrangeWithPatterns(
    std::map<unsigned long, unsigned long> &classes,
    std::vector<FPattern<unsigned long>> &patterns) {
    assert(root);

    //Create a tmp adjancency table from the patterns
    std::map<unsigned long, std::vector<std::pair<unsigned long, long>>> tmpMap;
    for (auto &pattern : patterns) {
        for (auto element : pattern.patternElements) {
            if (!tmpMap.count(element)) {
                tmpMap.insert(make_pair(element,
                                        std::vector <
                                        std::pair<unsigned long, long >> ()));
            }
            auto &vector = tmpMap.find(element)->second;
            for (auto element2 : pattern.patternElements) {
                if (element != element2) {
                    vector.push_back(std::make_pair(element2, pattern.support));
                }
            }
        }
    }
    std::map < unsigned long,
        std::vector<unsigned long>> neighbours;
    //Construct the table from the tmp table
    for (auto &pair : tmpMap) {
        //Clean the vector of elements
        std::sort(pair.second.begin(), pair.second.end(), sorterKeyFirst);
        //Remove the duplicates
        std::vector<std::pair<unsigned long, long>> newVector;
        long prev = -1;
        for (auto el : pair.second) {
            if (el.first != prev) {
                newVector.push_back(el);
                prev = el.first;
            }
        }
        //Sort by support now
        std::sort(newVector.begin(), newVector.end(), sorterSupportFirst);

        //Create a new pair in neighbours
        std::vector<unsigned long> onlyClasses;
        for (auto &p : newVector) {
            onlyClasses.push_back(p.first);
        }
        neighbours.insert(make_pair(pair.first, onlyClasses));
    }

    //Rearrange the children
    rearrangeChildrenWithPatterns(root, classes, neighbours);

    //Assign a number to all the terms in the tree
    long counterID = 0;
    assignID(root, counterID);

    //Compute the transitive closure and deallocate all the trees
    outputSubclasses.clear();
    transitiveClosure(outputSubclasses, root);
}

string getText(map<long, string> &map, long hash) {
    string out;
    if (map.count(hash)) {
        out = map.find(hash)->second;
    } else {
        if (hash == SchemaExtractor::HASHCLASS) {
            out = string(S_RDFS_CLASS);
        } else {
            out = to_string(hash);
        }
    }
    return out;
}

void SchemaExtractor::serializeNode(boost::iostreams::filtering_ostream &out,
                                    ExtNode *node) {
    string sk = getText(hashMappings, node->key);
    if (node->parent != NULL) {
        string spk = getText(hashMappings, node->parent->key);
        out << sk << "\t" << spk << endl;
    } else {
        out << sk << "\tNULL" << endl;
    }

    ExtNode *s = node->sibling;
    if (s != NULL) {
        serializeNode(out, s);
    }
    if (node->child != NULL) {
        serializeNode(out, node->child);
    }
}

void SchemaExtractor::serialize(string outputFile) {
    std::ofstream fout(outputFile, ios_base::binary);
    boost::iostreams::filtering_ostream out;
    if (boost::algorithm::ends_with(outputFile, ".gz")) {
        out.push(boost::iostreams::gzip_compressor());
    }
    out.push(fout);

    serializeNode(out, root);

    out.reset();
    fout.close();
}
