#include <cstdio>

#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
#include <time.h>


extern "C" {
int yyparse(void);
FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
//how to run this code?
//g++ -std=c++11 -o main main.cpp -lfl -ly -lglog
void InitGoogleLog(char *argv) {
  FLAGS_logtostderr = true;
  FLAGS_colorlogtostderr = true;
  google::InitGoogleLogging(argv);
  // LOG(INFO) << "glog started!";
}

void InputCommand(char *input, const int len) {
  memset(input, 0, len);
  printf("minisql > ");
  int i = 0;
  char ch;
  while ((ch = getchar()) != ';') {
    input[i++] = ch;
  }
  input[i] = ch;  // ;
  getchar();      // remove enter
}

int main(int argc, char **argv) {
  InitGoogleLog(argv[0]);
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];
  // executor engine
  ExecuteEngine engine;
  // for print syntax tree
  TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  uint32_t syntax_tree_id = 0;

  while (1) {
    // read from buffer
    InputCommand(cmd, buf_size);
    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    //若为insert，由于可能为批量插入，需要记录总时间
    clock_t start, end;
    int flag = 0;
    double total_time = 0;
    //若为插入语句，开始计时
    if (MinisqlGetParserRootNode()->type_ == kNodeInsert) {
      if(flag==0)
      {start = clock();
      flag = 1;}
      else {
        end = clock();
        total_time += (double)(end - start) / 1000;
        end = 0;
        start = clock();
      }
    }
    //若不是插入语句，结束计时
    if (MinisqlGetParserRootNode()->type_ != kNodeInsert && flag == 1) {
      end = clock();
      total_time += (double)(end - start) / 1000;
      printf("Total time: %f ms\n", total_time);
      flag = 0;
      start = 0;
      end = 0;
    }
    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      // Comment them out if you don't need to debug the syntax tree
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
    }
    


    auto result = engine.Execute(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    engine.ExecuteInformation(result);
    if (result == DB_QUIT) {
      break;
    }
  }
  return 0;
}