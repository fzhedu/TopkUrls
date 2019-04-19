//============================================================================
// Name        : Top-K-url.cpp
// Author      : fzh
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

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
#define TOPK 100
int MOD = 1e9 + 7;
int tmp_file_num = 0;
#define FILE_SIZE 128  // 100 * 1024 * 1024  // 100 MB
set<string> overflow_partition_file;

// typedef unsigned long long uint64_t;
string data_set_file = "data_set.txt";

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

bool GenData(uint64_t size_gb) {
  assert(size_gb <= 500);
  string url;
  stringstream sstr;
  int random_num = 0;
  uint64_t size = (size_gb << 10), temp = 0;
  ofstream file(data_set_file.c_str());
  if (file.fail()) {
    cout << "error: falied to open the file data_set.txt";
    file.close();
    return false;
  }
  while (temp < size) {
    random_num = rand() % 100;
    sstr << random_num;
    url = "https://github.com/fzhedu/" + sstr.str();
    file << url << endl;
    temp += url.size();
    sstr.str("");
  }
  file.close();
  return true;
}

struct cmp {
  template <typename T, typename U>
  bool operator()(T const &left, U const &right) {
    if (left.second > right.second) return true;  // minimal second first
    return false;
  }
};
uint64_t HashStr(string str, int seed) {
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
unsigned GetPartitionNum(const char *filename) {
  uint64_t size = FileSize(filename);
  return size / FILE_SIZE * 2 + 11;
}
// approximate size of a map
unsigned GetMapSize(map<string, int> *mmap) {
  return sizeof(*mmap) + mmap->size() * URL_MAX_SIZE;
}
/*
 * Hash partition the data in the map @url_count, then write each bucket to a
 * partition file. Note the seed is different in each calling so that the
 * over-sized file can be split into many smaller files.
 */
bool WritePartition(map<string, int> *url_count, int par_num, int seed) {
  map<string, int>::iterator iter;
  vector<pair<string, int>> bucket[par_num];
  int hash_value = 0;
  // hash partition the data from the map
  for (iter = url_count->begin(); iter != url_count->end(); iter++) {
    hash_value = HashStr(iter->first, seed) % par_num;
    bucket[hash_value].push_back(pair<string, int>(iter->first, iter->second));
#if DEGUG
    cout << iter->first << " -> " << iter->second << " hash = " << hash_value
         << endl;
#endif
  }
  stringstream sstr;
  // write each bucket to a partition file
  for (int par_id = 0; par_id < par_num; ++par_id) {
    if (bucket[par_id].size() > 0) {
      sstr.str("");
      sstr << "./tmp/partition" << (tmp_file_num + par_id);
      // write to a paritition file
      ofstream fout(sstr.str(), ios::app);
      if (fout.fail()) {
        cout << "error: create or open partition file failed!" << endl;
        fout.close();
        return false;
      }
      // if the partition file is too large, then it has to be repartitioned
      if (FileSize(sstr.str().c_str()) > FILE_SIZE) {
        overflow_partition_file.insert(sstr.str());
      }
      for (unsigned j = 0; j < bucket[par_id].size(); ++j) {
        fout << bucket[par_id][j].first << " " << bucket[par_id][j].second
             << endl;
      }
      fout.close();
    }
  }
  return true;
}
/*
 * Hash partition the raw dataset to many small partition files.
 * First, the raw data is reduced using a map to get partial results, then the
 * reduced data are written to partition files on the disk. Due to the
 * limit memory, if the memory footprint of the map  exceed the limit size, i.e,
 * FILE_SIZE, then the map should be flashed to the disk.
 */
bool PartitionRawData() {
  ifstream file(data_set_file.c_str());
  if (file.fail()) {
    cout << "error: failed to open the file data_set.txt";
    file.close();
    return false;
  }
  char url[URL_MAX_SIZE];
  int par_num = GetPartitionNum(data_set_file.c_str()),
      seed = tmp_file_num + 111;
  map<string, int> url_count;
  while (file.getline(url, URL_MAX_SIZE)) {
    url_count[url]++;

    if (GetMapSize(&url_count) > FILE_SIZE) {  // exceed the limit
      WritePartition(&url_count, par_num, seed);
      url_count.clear();
    }
  }
  file.close();
  // partition the remaining data in the map
  if (!url_count.empty()) {
    WritePartition(&url_count, par_num, seed);
    url_count.clear();
  }
  tmp_file_num += par_num;
  return true;
}
/*
 * Repartition a partition file that exceeds the limit of FILE_SIZE
 * The main function is similar to the PartitionRawData().
 * Note: the data format here differs that in the raw data set file
 * data format: "url count" per line
 */
bool Repartition(const char *filename) {
  map<string, int> url_count;
  string url;
  int count;
  int par_num = GetPartitionNum(filename), seed = tmp_file_num + 111;
  ifstream fin(filename, ios::in);
  if (fin.fail()) {
    fin.close();
    cout << "error: read file failed!!" << filename << endl;
    return false;
  }
  while (fin >> url >> count) {
    if (fin.eof()) break;
    url_count[url] += count;
    if (GetMapSize(&url_count) > FILE_SIZE) {  // exceed the limit
      WritePartition(&url_count, par_num, seed);
      url_count.clear();
    }
  }
  fin.close();
  // partition the remaining data in the map
  if (!url_count.empty()) {
    WritePartition(&url_count, par_num, seed);
    url_count.clear();
  }
  tmp_file_num += par_num;
  // remove the over-sized partition file
  RmFile(filename);
  return true;
}
bool HandleOverflowedPartition() {
  while (!overflow_partition_file.empty()) {
    string filename = *(overflow_partition_file.begin());
    Repartition(filename.c_str());
    overflow_partition_file.erase(overflow_partition_file.begin());
  }
  return true;
}
void TopKOfAMap(
    map<string, int> *mymap,
    priority_queue<pair<string, int>, vector<pair<string, int>>, cmp> *topk) {
  map<string, int>::iterator iter;
  int i = 0;
  for (iter = mymap->begin(); i < TOPK && iter != mymap->end(); ++iter, ++i) {
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
void MergeTopK(priority_queue<pair<string, int>, vector<pair<string, int>>,
                              cmp> *global_topk,
               priority_queue<pair<string, int>, vector<pair<string, int>>,
                              cmp> *partical_topk) {
  while (!partical_topk->empty()) {
    global_topk->push(partical_topk->top());
    partical_topk->pop();
  }
  while (global_topk->size() > TOPK) {
    global_topk->pop();
  }
}

bool ReduceAllPartition() {
  map<string, int> url_count;
  map<string, int>::iterator iter;
  stringstream sstr;
  string url;
  int count;
  ofstream fout("output.txt", ios::out);
  if (fout.fail()) {
    cout << "error: create output file failed!" << endl;
    fout.close();
    return false;
  }
  priority_queue<pair<string, int>, vector<pair<string, int>>, cmp> global_topk,
      partical_topk;
  // reduce each partition to get a partical topk, and merge with the global
  // topk.
  for (int file_id = 0; file_id < tmp_file_num; ++file_id) {
    sstr.str("");
    sstr << "./tmp/partition" << file_id;
    ifstream fin(sstr.str().c_str(), ios::in);
    if (fin.fail()) {
      fin.close();
      continue;
    }
    while (fin >> url >> count) {
      if (fin.eof()) break;
      url_count[url] += count;
    }
    fin.close();
    TopKOfAMap(&url_count, &partical_topk);
    MergeTopK(&global_topk, &partical_topk);
    url_count.clear();
    while (!partical_topk.empty()) {
      partical_topk.pop();
    }
  }
  // write global topk to the output.txt
  while (!global_topk.empty()) {
    fout << global_topk.top().first << " -> " << global_topk.top().second
         << endl;
    global_topk.pop();
  }
  fout.close();
  return true;
}

int main() {
  MkDir("./tmp");
  assert(FILE_SIZE > 127);
  overflow_partition_file.clear();
  GenData(10);
  // partition data into small parts
  PartitionRawData();
  // partitioned data that exceeds the limit
  HandleOverflowedPartition();
  // count top K for each partition
  ReduceAllPartition();
  // sleep(10000);
  RmFile("./tmp");
  return 0;
}
