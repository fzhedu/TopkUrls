#include "head.h"
bool GenData(uint64_t size_gb) {
  assert(size_gb <= 500);
  string url;
  stringstream sstr;
  uint64_t random_num = 0;
  uint64_t size = (size_gb << 30), temp = 0;
  sstr << "S" << SizeGB << "U" << UniqueUrl;
  data_set_file = data_set_file + sstr.str();
  ofstream file(data_set_file.c_str());
  if (file.fail()) {
    cout << "error: falied to open the file data_set.txt";
    file.close();
    return false;
  }
  while (temp < size) {
    sstr.str("");
    random_num = rand() % UniqueUrl;
    sstr << random_num;
    url = "https://github.com/fzhedu/" + sstr.str();
    file << url << endl;
    temp += url.size();
  }
  file.close();
  cout << size_gb << " GB data is generated successfully!" << endl;
  return true;
}
int main() {
  cout << "Please input two integers: S (size of GB) U (max of unique urls)( 0 "
          "< S < 500, 0 < U < max of uint32)" << endl;
  cin >> SizeGB;
  cin >> UniqueUrl;
  assert(0 < SizeGB && SizeGB < 500);
  assert(0 < UniqueUrl && UniqueUrl < UINT32_MAX);
  GenData(SizeGB);
  return 0;
}
