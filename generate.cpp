/*
  This program can Generate input file.
*/

#include <fstream>
#include <ios>
#include <string>
#include <iostream>

using namespace std;

int main(int argc, char **argv)
{

  if (argc < 5)
  {
    std::cerr << "needs at least 4 arguments - output <cmd start end> " << std::endl;
    return -1;
  }

  string path(argv[1]);
  fstream fs(path, ios_base::out);

  for (int j = 2; j < argc; j+=3)
  {
    string operation(argv[j]);
    int start = atoi(argv[j + 1]);
    int end = atoi(argv[j + 2]);

    for (int i = start; i <= end; i++)
    {
      if (operation.compare("Query") == 0)
      {
        fs << operation << " " << i << " -> " << i << ":" << endl;
      }
      else
      {
        fs << operation << " " << i << endl;
      }
    }
  }

  fs.close();
}