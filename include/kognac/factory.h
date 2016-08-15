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

#ifndef FACTORY_H_
#define FACTORY_H_

#include <iostream>
#include <vector>
#include <memory>

#define DEFAULT_SIZE 1000

template<class TypeEl>
class Factory {
private:
    TypeEl **elements;
    int maxSize;
    int size;

public:
    Factory() :
        maxSize(DEFAULT_SIZE) {
        elements = new TypeEl*[maxSize];
        size = 0;
    }

    Factory(int maxSize) :
        maxSize(maxSize) {
        elements = new TypeEl*[maxSize];
        size = 0;
    }

    TypeEl *get() {
        if (size > 0) {
            return elements[--size];
        } else {
            return new TypeEl();
        }
    }

    void release(TypeEl *el) {
        if (size < maxSize) {
            elements[size++] = el;
        } else {
            delete el;
        }
    }

    ~Factory() {
        for (int i = 0; i < size; ++i) {
            delete elements[i];
        }
        delete[] elements;
    }
};

template<class TypeEl>
class PreallocatedFactory {
private:
    const int maxPreallocatedSize;
    int preallocatedSize;
    TypeEl *preallocatedElements;

    const int maxSize;
    int size;
    TypeEl **elements;

public:
    PreallocatedFactory(int maxSize, int preallocatedSize) :
        maxPreallocatedSize(preallocatedSize), maxSize(maxSize) {
        this->size = 0;
        elements = new TypeEl*[maxSize];
        this->preallocatedSize = preallocatedSize;
        preallocatedElements = new TypeEl[preallocatedSize];
    }

    void release(TypeEl *el) {
        if (size < maxSize) {
            elements[size++] = el;
        } else {
            deallocate(el);
        }
    }

    void deallocate(TypeEl *el) {
        if (el < preallocatedElements
                || el >= preallocatedElements + maxPreallocatedSize) {
            delete el;
        }
    }

    TypeEl *get() {
        if (preallocatedSize > 0) {
            preallocatedSize--;
            return preallocatedElements + preallocatedSize;
        } else {
            return new TypeEl;
        }
    }

    ~PreallocatedFactory() {
        for (int i = 0; i < size; ++i) {
            deallocate(elements[i]);
        }
        delete[] elements;
        delete[] preallocatedElements;
    }
};

template<class TypeEl>
class PreallocatedArraysFactory {
private:
    const int arraySize;

    const int maxSize;
    int size;
    TypeEl **elements;

    const int maxPreallocatedSize;
    int preallocatedSize;
    TypeEl *preallocatedElements;

public:
    PreallocatedArraysFactory(int arraySize, int maxSize, int preallocatedSize) :
        arraySize(arraySize), maxSize(maxSize), maxPreallocatedSize(
            preallocatedSize * arraySize) {
        this->size = 0;
        elements = new TypeEl*[maxSize];
        this->preallocatedSize = preallocatedSize;
        preallocatedElements = new TypeEl[maxPreallocatedSize]();
    }

    void release(TypeEl *el) {
        if (size < maxSize) {
            elements[size++] = el;
        } else {
            deallocate(el);
        }
    }

    TypeEl *get() {
        if (preallocatedSize > 0) {
            preallocatedSize--;
            return preallocatedElements + (preallocatedSize * arraySize);
        } else {
            return new TypeEl[arraySize]();
        }
    }

    void deallocate(TypeEl *el) {
        if (el < preallocatedElements
                || el >= preallocatedElements + maxPreallocatedSize) {
            delete[] el;
        }
    }

    ~PreallocatedArraysFactory() {
        for (int i = 0; i < size; ++i) {
            deallocate(elements[i]);
        }
        delete[] elements;
        delete[] preallocatedElements;
    }
};

template<class TypeEl>
class PreallocatedStratArraysFactory {
private:
    const int arraySize;
    const int maxSize;
    int size;
    TypeEl **elements;

    const int maxPreallocatedSize;
    int preallocatedSize;
    TypeEl *preallocatedElements;
    std::vector<std::unique_ptr<TypeEl[]> > prevPreallocatedEls;

public:
    PreallocatedStratArraysFactory(int arraySize, int maxSize, int preallocatedSize) :
        arraySize(arraySize), maxSize(maxSize), maxPreallocatedSize(
            preallocatedSize * arraySize) {
        this->size = 0;
        elements = new TypeEl*[maxSize];
        this->preallocatedSize = preallocatedSize;
        preallocatedElements = new TypeEl[maxPreallocatedSize]();
        prevPreallocatedEls.push_back(std::unique_ptr<TypeEl[]>(preallocatedElements));
    }

    void release(TypeEl *el) {
        if (size < maxSize) {
            elements[size++] = el;
        }
    }

    TypeEl *get() {
        //First see if I have some buffers in elements
        if (size > 0) {
            size--;
            return elements[size];
        } else if (preallocatedSize > 0) {
            preallocatedSize--;
            return preallocatedElements + (preallocatedSize * arraySize);
        } else {
            //Create another block
            preallocatedSize = maxPreallocatedSize / arraySize;
            preallocatedElements = new TypeEl[maxPreallocatedSize]();
            prevPreallocatedEls.push_back(std::unique_ptr<TypeEl[]>(preallocatedElements));
            return get();
        }
    }

    ~PreallocatedStratArraysFactory() {
        delete[] elements;
    }
};


#endif
