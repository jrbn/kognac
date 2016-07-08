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

#ifndef SCHEMAEXT_H_
#define SCHEMAEXT_H_

#include <kognac/setestimation.h>
#include <kognac/stringscol.h>
#include <kognac/hashmap.h>
#include <kognac/fpgrowth.h>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>
#include <string>
#include <vector>
#include <set>

using namespace std;

typedef google::dense_hash_map<const char *, vector<long>, hashstr, eqstr> SchemaMap;
typedef google::dense_hash_map<long, vector<long>*> NumericSchemaMap;
typedef google::dense_hash_map<long, vector<long>> NumericNPSchemaMap;
typedef google::dense_hash_set<const char*, hashstr, eqstr> TextualSet;

typedef google::dense_hash_map<long, long> DomainRangeMap;

typedef struct ExtNode {
    const long key;
    ExtNode *child;
    ExtNode *parent;
    ExtNode* sibling;
    long assignedID;
    int depth;
    ExtNode(const long k) : key(k), child(NULL), parent(NULL), sibling(NULL),
        assignedID(0), depth(0) {}
    static bool less(ExtNode *n1, ExtNode *n2) {
        return n1->depth > n2->depth;
    }
} ExtNode;

class SchemaExtractor {

private:
    bool isPresent(const long el, vector<long> &elements);

    void rearrangeTreeByDepth(ExtNode *node);

    bool isReachable(NumericSchemaMap &map, vector<long> &prevEls, long source,
                     long dest);

    bool isDirectSubclass(NumericSchemaMap &map, long subclass, long superclass);

    void addToMap(SchemaMap &map, const char *key, const char *value);

    void addToMap(SchemaMap &map, const char *key, const long value);

    void addToMap(NumericNPSchemaMap &map, const long key, const long value);

    ExtNode *buildTreeFromRoot(NumericNPSchemaMap &map, NumericSchemaMap &subclassMap,
                               const long root);

    void processClasses(SchemaMap &inputMap, NumericNPSchemaMap &outputMap);

    void processExplicitClasses(SchemaMap &inputMap, TextualSet &set);

    void transitiveClosure(NumericNPSchemaMap &map, ExtNode *root);

    void deallocateTree(ExtNode *node);

    void assignID(ExtNode *root, long &counter);

    void printTree(int padding, ExtNode *root);

    void serializeNode(boost::iostreams::filtering_ostream &out,
                       ExtNode *node);

    void serializeNodeBeginRange(boost::iostreams::filtering_ostream &out,
                                 ExtNode *node);

protected:
    StringCollection supportSubclasses;
    SchemaMap subclasses;

    StringCollection supportExplicitClasses;
    TextualSet explicitClasses;

    NumericNPSchemaMap outputSubclasses;

    DomainRangeMap domains;
    DomainRangeMap ranges;

    map<long, string> hashMappings;
    map<long, std::pair<long, long>> classesRanges;

    /*SetEstimation propertyCardinality;
    google::dense_hash_map<long, long> propertiesID;*/

    ExtNode *root;

public:
    static const long HASHCLASS;
    static const long HASHTYPE;

    SchemaExtractor();

    void extractSchema(char **triple);

    void merge(SchemaExtractor & schema);

    void prepare();

    void printTree() {
        printTree(0, root);
    }

    void serialize(string file);

    void rearrangeWithPatterns(std::map<unsigned long, unsigned long> &classes,
                               std::vector<FPattern<unsigned long>> &patterns);

    long getRankingProperty(const long property);

    bool hasDomain(const long hashProperty) const;

    long getDomain(const long hashProperty) const;

    bool hasRange(const long hashProperty) const;

    long getRange(const long hashProperty) const;

    std::set<string> getAllClasses() const;

    string getText(long id) const;

    void retrieveInstances(const long term, const vector<long> **output) const;

    void addClassesBeginEndRange(const long classId,
                                 const long start,
                                 const long end);

    ~SchemaExtractor();
};

#endif
