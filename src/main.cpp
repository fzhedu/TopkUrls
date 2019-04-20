//============================================================================
// Name        : Top-K-url.cpp
// Author      : fzh
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include "head.h"

unsigned GetPartitionNum(const char *filename) {
  uint64_t size = FileSize(filename);
  size = size / FILE_SIZE * 2 + 11;
  return size > MAX_FILE_NUM ? MAX_FILE_NUM : size;
}
// approximate size of a map
unsigned GetMapSize(map<string, uint64_t> *mmap) {
  return sizeof(*mmap) + mmap->size() * URL_MAX_SIZE;
}
/*
 * Hash partition the data in the map @url_count, then write each bucket to a
 * partition file. Note the seed is different in each calling so that the
 * over-sized file can be split into many smaller files.
 */
bool WritePartition(map<string, uint64_t> *url_count, uint64_t par_num,
                    uint64_t seed) {
  map<string, uint64_t>::iterator iter;
  vector<pair<string, uint64_t>> bucket[par_num];
  int hash_value = 0;
  // hash partition the data from the map
  for (iter = url_count->begin(); iter != url_count->end(); iter++) {
    hash_value = HashStr(iter->first, seed) % par_num;
    bucket[hash_value].push_back(
        pair<string, uint64_t>(iter->first, iter->second));
#if DEBUG_INFO
    cout << iter->first << " -> " << iter->second << " hash = " << hash_value
         << endl;
#endif
  }
  stringstream sstr;
  // write each bucket to a partition file
  for (int par_id = 0; par_id < par_num; ++par_id) {
    if (bucket[par_id].size() > 0) {
      sstr.str("");
      sstr << "./tmp/partition" << (TmpFileNum + par_id);
      // write to a partition file
      ofstream fout(sstr.str(), ios::app);
      if (fout.fail()) {
        cout << "error: create or open partition file failed!" << endl;
        fout.close();
        return false;
      }
      // if the partition file is too large, then it has to be repartitioned
      if (FileSize(sstr.str().c_str()) > FILE_SIZE) {
        OverflowedPartitionFile.insert(sstr.str());
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
 * The raw dataset are hash partitioned to many small partition files.
 * First, the raw data is reduced using a map to get partial results, then the
 * reduced data are written to partition files on the disk. Due to the
 * limit memory, if the memory footprint of the map  exceed the limit size, i.e,
 * FILE_SIZE, then the map should be flashed to the disk.
 * NOTE: a special case where the urls are so less that can reside the memory,
 * then the urls do not need to be flashed to parition files, but directly
 * output the results.
 */
uint8_t PartitionRawData() {
  stringstream sstr;
  sstr << "S" << SizeGB << "U" << UniqueUrl;
  data_set_file = data_set_file + sstr.str();
  uint8_t flashed = 0;
  ifstream file(data_set_file.c_str());
  if (file.fail()) {
    cout << "error: failed to open the file " << data_set_file.c_str() << endl;
    file.close();
    return 0;
  }
  char url[URL_MAX_SIZE];
  uint64_t par_num = GetPartitionNum(data_set_file.c_str()),
           seed = TmpFileNum + 111;
  map<string, uint64_t> url_count;
  // read raw data and partially reduce the occurrence of urls
  while (file.getline(url, URL_MAX_SIZE)) {
    url_count[url]++;
    // the map size exceeds the limit, then the map is flashed
    if (GetMapSize(&url_count) > FILE_SIZE / 2) {
      WritePartition(&url_count, par_num, seed);
      url_count.clear();
      flashed = 1;
    }
  }
  file.close();
  if (flashed) {
    // partition the remaining data in the map
    if (!url_count.empty()) {
      WritePartition(&url_count, par_num, seed);
      url_count.clear();
    }
    TmpFileNum += par_num;
  } else {
    // the less urls resided in memory can directly calculate topk in memory
    bool res = OutputTopkFromMap(&url_count, false);
    if (!res) {
      return 0;
    } else {
      return 2;
    }
  }
  return 1;
}
/*
 * Repartition a partition file that exceeds the limit of FILE_SIZE
 * The main function is similar to the PartitionRawData().
 * Note: the data format here differs that in the raw data set file
 * data format: "url count" per line
 */
bool Repartition(const char *filename) {
  map<string, uint64_t> url_count;
  string url;
  uint64_t count;
  uint64_t par_num = GetPartitionNum(filename), seed = TmpFileNum + 111;
  ifstream fin(filename, ios::in);
  if (fin.fail()) {
    fin.close();
    cout << "error: read file failed!!" << filename << endl;
    return false;
  }
#if DEBUG_INFO
  cout << "repartition " << fifilename << endl;
#endif
  while (fin >> url >> count) {
    if (fin.eof()) break;
    url_count[url] += count;
    if (GetMapSize(&url_count) > FILE_SIZE / 2) {  // exceed the limit
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
  TmpFileNum += par_num;
  // remove the over-sized partition file
  RmFile(filename);
  return true;
}
bool HandleOverflowedPartition() {
  while (!OverflowedPartitionFile.empty()) {
    string filename = *(OverflowedPartitionFile.begin());
    Repartition(filename.c_str());
    OverflowedPartitionFile.erase(OverflowedPartitionFile.begin());
  }
  return true;
}

void MergeTopK(
    priority_queue<pair<string, uint64_t>, vector<pair<string, uint64_t>>,
                   cmp> *global_topk,
    priority_queue<pair<string, uint64_t>, vector<pair<string, uint64_t>>,
                   cmp> *partical_topk) {
  while (!partical_topk->empty()) {
    global_topk->push(partical_topk->top());
    partical_topk->pop();
  }
  while (global_topk->size() > TopK) {
    global_topk->pop();
  }
}

bool ReduceAllPartition() {
  map<string, uint64_t> url_count;
  map<string, uint64_t>::iterator iter;
  stringstream sstr;
  string url;
  uint64_t count;
  sstr << "S" << SizeGB << "U" << UniqueUrl << "T" << TopK;
  string output = string("output.txt") + sstr.str();
  sstr.str("");
  ofstream fout(output.c_str(), ios::out);
  if (fout.fail()) {
    cout << "error: create " << output << " file failed!" << endl;
    fout.close();
    return false;
  }
  priority_queue<pair<string, uint64_t>, vector<pair<string, uint64_t>>, cmp>
      global_topk, partical_topk;
  // reduce each partition to get a partial topk, and merge with the global
  // topk.
  for (int file_id = 0; file_id < TmpFileNum; ++file_id) {
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
  cout << "Please input three integers: S (size of GB) U (max of unique urls) T"
          " (top k)(0 < S < 500, 0 < U < max of uint32, 0 < T < 10000)" << endl;
  cin >> SizeGB;
  assert(0 < SizeGB && SizeGB < 500);
  cin >> UniqueUrl;
  assert(0 < UniqueUrl && UniqueUrl < UINT32_MAX);
  cin >> TopK;
  assert(0 < TopK && TopK < 10000);
  MkDir("./tmp");
  assert(FILE_SIZE > 127);
  OverflowedPartitionFile.clear();
  uint8_t res = true;

  // partition data into small parts
  res = PartitionRawData();
  if (0 == res) {
    cout << "Partition raw data failed" << endl;
    return 1;
  } else if (2 == res) {
    cout << "Reduce OK for less urls" << endl;
    return 0;
  }
  cout << "Partition raw data is ok" << endl;
  // partitioned data that exceeds the limit
  res = HandleOverflowedPartition();
  if (!res) {
    cout << "Overflowed Partitions failed" << endl;
    return 1;
  }
  cout << "Overflowed Partitions are handled" << endl;
  // count top K for each partition
  res = ReduceAllPartition();
  if (!res) {
    cout << "Reduce Partitions failed" << endl;
    return 1;
  }
  cout << "Reduce OK" << endl;
  // sleep(10000);
  RmFile("./tmp");
  return 0;
}
