#ifndef STORE_H
#define STORE_H

#pragma once

#include <iostream>
#include<vector>
#include<memory>
#include "StateTracker.h"
using namespace std;

class Store
{
public:
    Store();
    ~Store();

public:
    virtual void Connect() = 0;
    virtual void Disconnect() = 0;
    virtual bool Set(string key, string data) = 0;
    virtual string Get(string key) = 0;
    // virtual void Migrate(string destinationIp, std::vector<string> keys) = 0;

private:

};

#endif