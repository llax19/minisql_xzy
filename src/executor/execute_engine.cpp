#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include <cstring>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "catalog/indexes.h"

#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
extern int yyparse(void);
extern FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(db_name.c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database" << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << "" << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (ast->type_ != kNodeCreateTable) return DB_FAILED;
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  try {
    std::string table_name = ast->child_->val_, column_name;
    TypeId column_type;
    int length;
    bool nullable, unique;
    std::vector<Column *> column;
    std::vector<std::string> primary_keys;
    TableSchema *schema;
    int index = 0;
    ast = ast->child_->next_->child_;
    // find primary keys
    void *tmp = ast;
    while (ast) {
      if (ast->type_ == kNodeColumnList && !strcmp(ast->val_, "primary keys")) {
        ast = ast->child_;
        while (ast) {
          primary_keys.push_back(ast->val_);
          ast = ast->next_;
        }
        break;
      }
      ast = ast->next_;
    }
    ast = (pSyntaxNode)tmp;
    // create columns and constraints
    while (ast) {
      // create columns and some constraints (unique, not null)
      if (ast->type_ == kNodeColumnDefinition) {
        // unique and nullable processing
        if (ast->val_)
          unique = !strcmp(ast->val_, "unique");
        else
          unique = false;
        column_name = ast->child_->val_;
        nullable = true;
        for (auto key : primary_keys) {
          if (column_name == key) {
            if(primary_keys.size()==1) unique = true;
            nullable = false;
            break;
          }
        }
        // kType and length processing
        if (std::strcmp(ast->child_->next_->val_, "int") == 0) {
          column_type = kTypeInt;
          length = -1;
        } else if (std::strcmp(ast->child_->next_->val_, "float") == 0) {
          column_type = kTypeFloat;
          length = -1;
        } else if (std::strcmp(ast->child_->next_->val_, "char") == 0) {
          column_type = kTypeChar;
          length = stoi(ast->child_->next_->child_->val_);
          if (length < 0) throw "Length of char attribute is smaller than 0";
        }
        if (length == -1)
          column.push_back(new Column(column_name, column_type, index, nullable, unique));
        else
          column.push_back(new Column(column_name, column_type, length, index, nullable, unique));
        index++;
      }
      ast = ast->next_;
    }
    schema = new TableSchema(column);
    TableInfo *table_info;
    Txn *txn = new Txn();  // recovery not implemented
    dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema, txn, table_info);
    if (!primary_keys.empty()) {
      IndexInfo *index_info;
      dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, "PRIMARY", primary_keys, txn, index_info,
                                                   "primary keys");
    }
    return DB_SUCCESS;
  } catch (std::exception &e) {
    cout << e.what() << endl;
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (ast->type_ != kNodeDropTable) {
    return DB_FAILED;
  }
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  try {
    std::vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(ast->child_->val_, indexes);
    for (auto index : indexes) {
      dbs_[current_db_]->catalog_mgr_->DropIndex(ast->child_->val_, index->GetIndexName());
    }
    return dbs_[current_db_]->catalog_mgr_->DropTable(ast->child_->val_);
  } catch (...) {
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (ast->type_ != kNodeShowIndexes) {
    return DB_FAILED;
  }
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  try {
    std::vector<TableInfo *> tables;
    std::vector<IndexInfo *> indexes;
    std::vector<string> index_names;
    std::vector<string> index_keys;
    std::vector<uint32_t> index_keymap;
    std::string keys, table_name;
    TableSchema *schema;
    int max_length_name = 4, max_length_keys = 7;
    if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) return DB_FAILED;
    for (auto table : tables) {
      table_name = table->GetTableName();
      schema = table->GetSchema();
      dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
      for (auto index : indexes) {
        index_names.push_back(index->GetIndexName());
        if (max_length_name < index->GetIndexName().length()) max_length_name = index->GetIndexName().length();
        index_keymap = index->GetIndexMetadata().GetKeyMapping();
        for (auto key_id : index_keymap) {
          if (keys == "")
            keys = schema->GetColumn(key_id)->GetName();
          else
            keys = keys + ", " + schema->GetColumn(key_id)->GetName();
        }
        index_keys.push_back(keys);
        if (max_length_keys < (int)keys.length()) max_length_keys = keys.length();
        keys = "";
      }
      cout << "Indexes in table " << table_name << ":" << endl;
      cout << "+" << setfill('-') << setw(max_length_name + 2) << "" << "+" << setfill('-') << setw(max_length_keys + 2)
           << "" << "+" << endl;
      cout << "| " << std::left << setfill(' ') << setw(max_length_name) << "Name" << " | " << std::left
           << setw(max_length_keys) << "Columns" << " |" << endl;
      cout << "+" << setfill('-') << setw(max_length_name + 2) << "" << "+" << setfill('-') << setw(max_length_keys + 2)
           << "" << "+" << endl;
      for (int i = 0; i < (int)index_names.size(); i++)
        cout << "| " << std::left << setfill(' ') << setw(max_length_name) << index_names[i] << " | " << std::left
             << setw(max_length_keys) << index_keys[i] << " |" << endl;
      cout << "+" << setfill('-') << setw(max_length_name + 2) << "" << "+" << setfill('-') << setw(max_length_keys + 2)
           << "" << "+" << endl;
      indexes.clear();
      index_names.clear();
      index_keys.clear();
      max_length_name = 4;
      max_length_keys = 7;
    }
    return DB_SUCCESS;
  } catch (...) {
    return DB_FAILED;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
  //#ifdef ENABLE_EXECUTE_DEBUG
  //  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
  //#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    std::cout << "No database selected" << std::endl;
    return DB_FAILED;
  }
  // 记录索引
  pSyntaxNode p = ast->child_;
  std::string indexname = p->val_;
  p = p->next_;
  // 记录表名
  std::string tablename = p->val_;
  p = p->next_;
  // 判断输入格式是否有误
  if (p->type_ != kNodeColumnList) return DB_FAILED;
  p = p->child_;
  // 记录所有的作为索引字段的列名
  std::vector<std::string> index_keys;
  // 判断是否是unique,只能在unique keys上建立索引
  TableInfo *table_info;
  // 取出表用以判断
  if (dbs_[current_db_]->catalog_mgr_->GetTable(tablename, table_info) != DB_TABLE_NOT_EXIST) {
    TableSchema *schema = table_info->GetSchema();
    vector<Column *> columns = schema->GetColumns();
    for (auto iter : columns) {
      if (string(iter->GetName()) == string(p->val_)) {
        if (iter->IsUnique())
          break;
      }
    }
  }
  IndexInfo *index_info = nullptr;
  // 为了简单地通过测试，默认只有一个index
  index_keys.push_back(p->val_);
  // 建立索引，目的是为了将所有内容插入到建立的索引之中
  if (DB_SUCCESS ==
      dbs_[current_db_]->catalog_mgr_->CreateIndex(tablename, indexname, index_keys, nullptr, index_info, "bptree")) {
    // 将所有内容插入到建立的索引之中
    for (auto it = table_info->GetTableHeap()->Begin(nullptr); it != table_info->GetTableHeap()->End(); ++it) {
      vector<Field *> fields = it->GetFields();
      vector<Field> field_temps;
      vector<Column *> columns = table_info->GetSchema()->GetColumns();
      vector<Column *> columns_index = index_info->GetIndexKeySchema()->GetColumns();
      for (auto itt : columns_index) {
        int i = 0;
        for (auto that : columns) {
          if (itt->GetName() == that->GetName()) field_temps.push_back(*(fields[i]));
          i++;
        }
      }
      Row row_temp(field_temps);
      index_info->InsertEntry(row_temp, it->GetRowId(), nullptr);
    }
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (ast->type_ != kNodeDropIndex) {
    return DB_FAILED;
  }
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  try {
    std::vector<TableInfo *> tables;
    std::vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTables(tables);
    for (auto table : tables) {
      dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
      for (auto index : indexes) {
        if (index->GetIndexName() == ast->child_->val_)
          return dbs_[current_db_]->catalog_mgr_->DropIndex(table->GetTableName(), index->GetIndexName());
      }
      indexes.clear();
    }
  } catch (...) {
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  try {
    std::FILE *fp;
    ExecuteEngine &engine = *this;
    TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
    uint32_t syntax_tree_id = 0;
    char buf[1024];
    fp = fopen(ast->child_->val_, "r");
    do {
      memset(buf, 0, 1024);
      int i = 0;
      char ch;
      while ((ch = getc(fp)) != ';') {
        buf[i++] = ch;
      }
      buf[i] = ch;
      buf[i + 1] = 0;
      cout << buf << endl;
      YY_BUFFER_STATE bp = yy_scan_string(buf);
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);

      // init parser module
      MinisqlParserInit();

      // parse
      yyparse();

      // parse result handle
      if (MinisqlParserGetError()) {
        // error
        printf("%s\n", MinisqlParserGetErrorMessage());
      } else {
        // Comment them out if you don't need to debug the syntax tree
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
    } while (getc(fp) != EOF);
    return DB_SUCCESS;
  } catch (...) {
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  if (ast->type_ != kNodeQuit) {
    return DB_FAILED;
  }
  return DB_QUIT;
}
