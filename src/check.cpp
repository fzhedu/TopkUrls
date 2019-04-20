#include "head.h"
/*
 * calculate the topk in memory and compare it with that possiblely calculated
 * in disk due to limited memory size.
 * NOTE:
 * (1) the unique urls can reside in memory
 * (2) the results from the above two cases maybe differ due to equal
 * occurrence of urls
 */
bool topk_in_memory() {
  stringstream sstr;
  sstr << "S" << SizeGB << "U" << UniqueUrl;
  data_set_file = data_set_file + sstr.str();

  ifstream file(data_set_file.c_str());
  if (file.fail()) {
    cout << "error: failed to open the file " << data_set_file.c_str() << endl;
    file.close();
    return false;
  }
  sstr.str("");
  char url[URL_MAX_SIZE];
  map<string, uint64_t> url_count;
  // count the occurrence of all urls
  while (file.getline(url, URL_MAX_SIZE)) {
    url_count[url]++;
  }
  file.close();
  // output topk from the url_count
  OutputTopkFromMap(&url_count, true);
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
  topk_in_memory();
  return 0;
}
