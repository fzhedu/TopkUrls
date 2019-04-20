
#ifndef HEAD_H_
#define HEAD_H_
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <string>
#include <algorithm>
#include <stdlib.h>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <map>
#include <queue>
#include <utility>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <set>
#include <vector>
using namespace std;

#define URL_MAX_SIZE 64
#define MAX_FILE_NUM 100000
#define FILE_SIZE (800 * 1024 * 1024)  //  800MB

uint64_t MOD = (1e9 + 7);
int TmpFileNum = 0;
set<string> OverflowedPartitionFile;

uint64_t TopK = 100;
uint64_t UniqueUrl = 111;
uint32_t SizeGB = 1;
// typedef unsigned long long uint64_t;
string data_set_file = "data_set.txt";

struct cmp {
  template <typename T, typename U>
  bool operator()(T const &left, U const &right) {
    if (left.second > right.second) return true;  // minimal second first
    return false;
  }
};
void TopKOfAMap(map<string, uint64_t> *mymap,
                priority_queue<pair<string, uint64_t>,
                               vector<pair<string, uint64_t>>, cmp> *topk) {
  map<string, uint64_t>::iterator iter;
  int i = 0;
  for (iter = mymap->begin(); i < TopK && iter != mymap->end(); ++iter, ++i) {
    topk->push(*iter);
  }
  // mymap.size > TOPK
  for (; iter != mymap->end(); ++iter) {
    if (iter->second > topk->top().second) {
      topk->pop();
      topk->push(*iter);
    }
  }
}
bool OutputTopkFromMap(map<string, uint64_t> *url_count, bool check) {
  stringstream sstr;
  sstr << "S" << SizeGB << "U" << UniqueUrl << "T" << TopK;
  string output = string("output.txt") + sstr.str();
  if (check) {
    output = output + string("_check");
  }
  sstr.str("");
  ofstream fout(output.c_str(), ios::out);
  if (fout.fail()) {
    cout << "error: create " << output << " file failed!" << endl;
    fout.close();
    return false;
  }
  // calculate the top k
  priority_queue<pair<string, uint64_t>, vector<pair<string, uint64_t>>, cmp>
      global_topk;
  TopKOfAMap(url_count, &global_topk);

  // write global topk to the output.txt
  while (!global_topk.empty()) {
    fout << global_topk.top().first << " -> " << global_topk.top().second
         << endl;
    global_topk.pop();
  }
  fout.close();
  return true;
}

void MkDir(const char *dir) {
  if (access(dir, 0) == -1) {
    cout << dir << " is not existing" << endl;
    cout << "now make it" << endl;
    int flag = mkdir(dir, 0777);
    if (flag == 0) {
      cout << "make successfully" << endl;
    } else {
      cout << "make errorly" << endl;
    }
  } else {
    assert("Please delete the tmp dir first");
  }
}
void Getfilepath(const char *path, const char *filename, char *filepath) {
  strcpy(filepath, path);
  if (filepath[strlen(path) - 1] != '/') strcat(filepath, "/");
  strcat(filepath, filename);
}

bool RmFile(const char *path) {
  DIR *dir;
  struct dirent *dirinfo;
  struct stat statbuf;
  char filepath[256] = {0};
  lstat(path, &statbuf);

  if (S_ISREG(statbuf.st_mode)) {  // delete regular file
    remove(path);
    return true;
  } else if (S_ISDIR(statbuf.st_mode)) {  // a dir
    if ((dir = opendir(path)) == NULL) return 1;
    while ((dirinfo = readdir(dir)) != NULL) {
      Getfilepath(path, dirinfo->d_name, filepath);
      if (strcmp(dirinfo->d_name, ".") == 0 ||
          strcmp(dirinfo->d_name, "..") == 0)  // special dir
        continue;
      RmFile(filepath);
      rmdir(filepath);
    }
    closedir(dir);
  }
  rmdir(path);  // rm path

  cout << "rm dir" << path << " successfully" << endl;
  return 0;
}

uint64_t HashStr(string str, uint64_t seed) {
  int length = str.size();
  uint64_t temp = ((uint64_t)str[0]) % MOD;
  for (int i = 1; i < length; i++) {
    temp = (temp * seed + (uint64_t)str[i]) % MOD;
  }
  return temp;
}
uint64_t FileSize(const char *filename) {
  struct stat info;
  stat(filename, &info);
  return info.st_size;
}
#endif  //  HEAD_H_
