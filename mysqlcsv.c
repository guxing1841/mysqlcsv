/*
 * Copyright (C) Changrong Zhou
 * Last modify at 2011-11-15
 */

#include <my_config.h>
#include <getopt.h>
#include <dirent.h>
#include <sys/wait.h>
#include <unistd.h>

#include <my_global.h>
#include <mysql.h>
#include "common.h"
#define MC_VERSION "1.0.0"
static char *progname = NULL;
static char workdir[PATH_MAX+1];
static MYSQL mysql_connect, *mysql = NULL;
static MYSQL_ROW row;
static unsigned long server_version = 0;
static int opt_dump = 0;
static int opt_import = 0;
static int opt_force = 0;
static char *opt_directory = NULL;
//static char *opt_default_character_set = (char*) MYSQL_DEFAULT_CHARSET_NAME;
static char *opt_default_character_set = (char*) MYSQL_UNIVERSAL_CLIENT_CHARSET;
static char *opt_file = NULL;
static int opt_verbose = 0;
static int opt_all_databases = 0;
static int opt_databases = 0;
static int opt_disable_keys = 0;
static char *opt_defaults_file = NULL;
static int opt_drop_table = 0;
static int opt_ignore = 0;
static int opt_debug = 0;
static int opt_replace = 0;
static int opt_lock_all = 0;
static int opt_lock = 0;
static int opt_quick = 0;
static int opt_no_data = 0;
static int opt_master_data = 0;
static int opt_compress = 0;
static int opt_single_transaction = 0;
static char *opt_user = NULL;
static int opt_getpass = 0;
static char *opt_password = NULL;
static char *opt_host = NULL;
static unsigned int opt_port = 0;
static char *opt_unix_socket = NULL;
static int opt_gzip = 0;
static int opt_function = 0;
static int opt_procedure = 0;
static int opt_trigger = 0;
static int opt_pipe = 0;
static DHASH *ignore_databases = NULL;
static DHASH *ignore_tables = NULL;
static DHASH *ignore_functions = NULL;
static DHASH *ignore_procedures = NULL;
static DHASH *ignore_triggers = NULL;
static DHASH *ignore_data_types = NULL;
static DHASH *ignore_data_tables = NULL;
static DHASH *ignore_data_databases = NULL;
static DSTRING* sql = NULL;
static DSTRING* dstring = NULL;
static char *databases_where = NULL;
static char *tables_where = NULL;
static char *where = NULL;
static char *databases_pcre = NULL;
static char *tables_pcre = NULL;
static char *tables_form_default_pcre = "^(?i:CREATE\\s+TABLE\\s+)";
static char *tables_form_replace_pcre = "^(?i:(CREATE\\s+TABLE\\s+)(?!IF\\s+NOT\\s+EXISTS\\s+))";
static char *tables_form_view_pcre = "^(?i:CREATE\\s+(ALGORITHM\\s*=\\s*\\S+\\s+)?(DEFINER\\s*=\\s*`.+?`@`.+?`\\s+)?(SQL SECURITY .+?\\s+)?VIEW\\s+)";
static int opt_view = 0;
static char *tables_form_pcre = NULL;
static char *functions_pcre = NULL;
static char *procedures_pcre = NULL;
static char *triggers_pcre = NULL;
static char *functions_form_pcre = NULL;
static char *procedures_form_pcre = NULL;
static char *triggers_form_pcre = NULL;
static pcre *databases_re = NULL;
static pcre *tables_re = NULL;
static pcre *tables_form_re = NULL;
static pcre *tables_form_replace_re = NULL;
static pcre *functions_re = NULL;
static pcre *procedures_re = NULL;
static pcre *triggers_re = NULL;
static pcre *functions_form_re = NULL;
static pcre *procedures_form_re = NULL;
static pcre *triggers_form_re = NULL;
static char *opt_tmpdir = NULL;
static DSTRING *fifo_file = NULL;

typedef struct mc_fh_s
{
	int fd;
	FILE *file;
	gzFile gzfile;
	int pipe_pid;
} mc_fh_t;

typedef struct mc_ff_s
{
	mc_fh_t *fh;
	int (*write) (mc_fh_t *fh, const void *buf, size_t count);
	int (*close) (mc_fh_t *fh);
} mc_ff_t;

static int child_kill = 0;
static int pid = -1;
static void sighandler(int signo)
{
	switch (signo)
	{
		case SIGINT:
		case SIGTERM:
		case SIGPIPE:
			child_kill = 1;
			if (pid > 0)
			{
				kill(pid, signo);
			}
			break;
		default:
			break;
	}
}
/*
static int mc_f_write(mc_fh_t *fh, const void *buf, size_t count)
{
	return f_write(fh->file, buf, count);
}
*/
static int mc_fd_write(mc_fh_t *fh, const void *buf, size_t count)
{
	return fd_write(fh->fd, buf, count);
}

static int mc_gz_write(mc_fh_t *fh, const void *buf, size_t count)
{
	return gz_write(fh->gzfile, buf, count);
}


static int mc_fd_close(mc_fh_t *fh)
{
	return close(fh->fd);
}

static int mc_gz_close(mc_fh_t *fh)
{
	return gzclose(fh->gzfile);
}


static int mc_file_open(mc_ff_t *ff, const char *filename)
{
	int fd = open(filename, O_CREAT|O_TRUNC|O_EXCL|O_WRONLY, 0644);
	if (fd == -1)
	{
		log_error("Error: can't open '%s': %s!\n", filename, strerror(errno));
		return MC_FALSE;
	}
	if (opt_gzip)
	{
		char mode[10];
		snprintf(mode, 10, "wb%d", opt_gzip);
		ff->fh->gzfile = gzdopen(fd, mode);
		if (ff->fh->gzfile == NULL)
		{
			close(fd);
			log_error("Error: can't gzdopen '%s': %s!\n", filename, strerror(errno));
			return MC_FALSE;
		}
		ff->fh->fd = fd;
		ff->write = mc_gz_write;
		ff->close = mc_gz_close;
	}
	else
	{
		ff->fh->fd = fd;
		ff->write = mc_fd_write;
		ff->close = mc_fd_close;
	} 
	return MC_TRUE;
}

static int dump_mysql_form(const char *filename, const char *str, size_t len)
{
	mc_fh_t fh;
	mc_ff_t ff;
	ff.fh = &fh;
	int ret = MC_TRUE;
	if (opt_verbose>1)
		log_error("Open file: '%s'\n", filename);
	if (mc_file_open(&ff, filename) == MC_FALSE)
		return MC_FALSE;

	if (ff.write(ff.fh, str, len) == MC_FALSE)
		ret = MC_FALSE;
	if (opt_verbose>1)
		log_error("Close file: '%s'\n", filename);
	ff.close(ff.fh);
	return ret;
}

static int v_mysql_query(MYSQL *mysql, const char *sql)
{
	int ret;
	if (opt_verbose>1)
		log_error("Query: %s\n", sql);
	double s, u;
	s = my_time();
	ret = mysql_query(mysql, sql);
	u = my_time() - s;
	if (opt_verbose>1)
		log_error("Use time: %.3f sec\n", u);
	return ret;
}

static int db_connect()
{
	mysql_init(&mysql_connect);
	if (opt_compress)
		mysql_options(&mysql_connect, MYSQL_OPT_COMPRESS, NULL);
	if (opt_defaults_file != NULL)
		mysql_options(&mysql_connect, MYSQL_READ_DEFAULT_FILE, opt_defaults_file);
	//mysql_options(&mysql_connect, MYSQL_READ_DEFAULT_GROUP, "client");
	//mysql_options(&mysql_connect, MYSQL_READ_DEFAULT_GROUP, "mysqlcsv");
	mysql_options(&mysql_connect, MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);
	//if (opt_default_character_set != NULL)
	mysql_options(&mysql_connect, MYSQL_SET_CHARSET_NAME, opt_default_character_set);
	if (opt_verbose)
		log_error("Connect to %s@%s:%d\n", opt_user == NULL ? "" : opt_user, opt_host, opt_port);

	if ((mysql = mysql_real_connect(&mysql_connect,opt_host,opt_user,opt_password,NULL,opt_port,opt_unix_socket,0)) == NULL)
	{
		log_error("Error: can't connect: %s\n", mysql_error(&mysql_connect));
		return MC_FALSE;
	};

	server_version = mysql_get_server_version(mysql);
	if (opt_verbose)
		log_error("server version: %lu\n", server_version);

	if (opt_verbose)
		log_error("Charset: %s\n", mysql_character_set_name(mysql));
	//mysql_set_character_set(mysql, opt_default_character_set);
	if (opt_debug)
	{
		/*
		log_error("mysql->packet_length => %lu\n", mysql->packet_length);
		log_error("mysql->port => %u\n", mysql->port);
		*/
		MYSQL_ROW row;
		MYSQL_RES *results = NULL;
		unsigned long *lengths;
		unsigned int num_fields;
		//unsigned int i;
	
		if (v_mysql_query(mysql, "SHOW VARIABLES") != 0)
		{
			log_error("Error: can't query: %s\n", mysql_error(mysql));
			goto end_debug;
		}
		results = mysql_store_result(mysql);
		if (results == NULL)
		{
			log_error("Error: can't store result: %s\n", mysql_error(mysql));
			goto end_debug;
		}
		num_fields = mysql_num_fields(results);
		if (num_fields != 2)
		{
			log_error("Error: number of fields is miss match\n");
			goto end_debug;
		}
		log_error("%-35s %-60s\n", mysql_fetch_field_direct(results, 0)->name, mysql_fetch_field_direct(results, 1)->name);
		while ((row = mysql_fetch_row(results)) != NULL)
		{
			lengths = mysql_fetch_lengths(results);
			log_error("%-35.*s %-60.*s\n", (int) lengths[0], row[0], (int) lengths[1], row[1]);
		}
		end_debug:
			if (results != NULL)
				mysql_free_result(results);
	}
	return MC_TRUE;
}

static int db_disconnect()
{
	if (mysql != NULL)
	{
		mysql_close(mysql);
		mysql = NULL;
	}
	return MC_TRUE;
}


static int is_ignore_table(const char *table, const char *database)
{
	int ret = MC_FALSE;
	DSTRING *db_table = NULL;
	if (database != NULL)
	{
		db_table = dynamic_string_new(128);
		dynamic_string_append(db_table, database);
		dynamic_string_append(db_table, ".");
		dynamic_string_append(db_table, table);
		dynamic_string_append_char(db_table, '\0');
	}
	if ((tables_re != NULL \
		&& ((database != NULL &&  mc_pcre_exec(tables_re, db_table->data, strlen(db_table->data))<0) \
		&&  mc_pcre_exec(tables_re, table, strlen(table))<0)) \
		|| (ignore_tables != NULL \
		&& ((database != NULL && dynamic_hash_haskey(ignore_tables, db_table->data, db_table->len-1) == MC_TRUE) \
		|| dynamic_hash_haskey(ignore_tables, table, strlen(table)) == MC_TRUE)))
	{
		if (opt_verbose > 1)
			log_error("Ignore table %s\n", db_table == NULL ? table : db_table->data);
		ret = MC_TRUE;
	}
	if (db_table != NULL)
		dynamic_string_destroy(db_table);
	return ret;
}

static int is_ignore_function(const char *function, const char *database)
{
	int ret = MC_FALSE;
	DSTRING *db_function = NULL;
	if (database != NULL)
	{
		db_function = dynamic_string_new(128);
		dynamic_string_append(db_function, database);
		dynamic_string_append(db_function, ".");
		dynamic_string_append(db_function, function);
		dynamic_string_append_char(db_function, '\0');
	}
	if ((functions_re != NULL \
		&& ((database != NULL && mc_pcre_exec(functions_re, db_function->data, strlen(db_function->data))<0) \
		&& mc_pcre_exec(functions_re, function, strlen(function))<0)) \
		|| (ignore_functions != NULL \
		&& ((database != NULL && dynamic_hash_haskey(ignore_functions, db_function->data, db_function->len-1) == MC_TRUE) \
		|| dynamic_hash_haskey(ignore_functions, function, strlen(function)) == MC_TRUE)))
	{
		if (opt_verbose > 1)
			log_error("Ignore function %s\n", db_function == NULL ? function : db_function->data);
		ret = MC_TRUE;
	}
	if (db_function != NULL)
		dynamic_string_destroy(db_function);
	return ret;
}

static int is_ignore_procedure(const char *procedure, const char *database)
{
	int ret = MC_FALSE;
	DSTRING *db_procedure = NULL;
	if (database != NULL)
	{
		db_procedure = dynamic_string_new(128);
		dynamic_string_append(db_procedure, database);
		dynamic_string_append(db_procedure, ".");
		dynamic_string_append(db_procedure, procedure);
		dynamic_string_append_char(db_procedure, '\0');
	}
	if ((procedures_re != NULL \
		&& ((database != NULL && mc_pcre_exec(procedures_re, db_procedure->data, strlen(db_procedure->data))<0) \
		&& mc_pcre_exec(procedures_re, procedure, strlen(procedure))<0)) \
		|| (ignore_procedures != NULL \
		&& ((database != NULL && dynamic_hash_haskey(ignore_procedures, db_procedure->data, db_procedure->len-1) == MC_TRUE) \
		|| dynamic_hash_haskey(ignore_procedures, procedure, strlen(procedure)) == MC_TRUE)))
	{
		if (opt_verbose > 1)
			log_error("Ignore procedure %s\n", db_procedure == NULL ? procedure : db_procedure->data);
		ret = MC_TRUE;
	}
	if (db_procedure != NULL)
		dynamic_string_destroy(db_procedure);
	return ret;
}

static int is_ignore_trigger(const char *trigger, const char *database)
{
	int ret = MC_FALSE;
	DSTRING *db_trigger = NULL;
	if (database != NULL)
	{
		db_trigger = dynamic_string_new(128);
		dynamic_string_append(db_trigger, database);
		dynamic_string_append(db_trigger, ".");
		dynamic_string_append(db_trigger, trigger);
		dynamic_string_append_char(db_trigger, '\0');
	}
	if ((triggers_re != NULL \
		&& ((database != NULL && mc_pcre_exec(triggers_re, db_trigger->data, strlen(db_trigger->data))<0) \
		&& mc_pcre_exec(triggers_re, trigger, strlen(trigger))<0)) \
		|| (ignore_triggers != NULL \
		&& ((database != NULL && dynamic_hash_haskey(ignore_triggers, db_trigger->data, db_trigger->len-1) == MC_TRUE) \
		|| dynamic_hash_haskey(ignore_triggers, trigger, strlen(trigger)) == MC_TRUE)))
	{
		if (opt_verbose > 1)
			log_error("Ignore trigger %s\n", db_trigger == NULL ? trigger : db_trigger->data);
		ret = MC_TRUE;
	}
	if (db_trigger != NULL)
		dynamic_string_destroy(db_trigger);
	return ret;
}


static int is_ignore_table_data(const char *table, const char *database)
{
	MYSQL_RES *results = NULL;
	MYSQL_ROW row;
	unsigned long *lengths;
	DSTRING *sql;
	DSTRING *table_type;
	/* this sql is not global variable */
	DSTRING *db_table = NULL;
	int ret = MC_FALSE;
	//return ret;
	if (ignore_data_tables != NULL)
	{
		if (database != NULL)
		{
			db_table = dynamic_string_new(128);
			dynamic_string_append(db_table, database);
			dynamic_string_append(db_table, ".");
			dynamic_string_append(db_table, table);
			dynamic_string_append_char(db_table, '\0');
		}
		if ((database != NULL && dynamic_hash_haskey(ignore_data_tables, db_table->data, db_table->len-1) == MC_TRUE) || dynamic_hash_haskey(ignore_data_tables, db_table->data, db_table->len-1) == MC_TRUE)
			return MC_TRUE;
		if (db_table != NULL)
			dynamic_string_destroy(db_table);
	}
	sql = dynamic_string_new(128);
	dynamic_string_append(sql, "SHOW TABLE STATUS WHERE Name = '");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "'");
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	row = mysql_fetch_row(results);
	if (row == NULL)
	{
		log_error("Warning: table `%s` status is empty\n", table);
		ret = MC_FALSE;
		goto end;
	}
	lengths = mysql_fetch_lengths(results);
	table_type = dynamic_string_new(128);
	if (row[1] == NULL)
	{
		dynamic_string_append(table_type, "VIEW");
		dynamic_string_append_char(table_type, '\0');
	}
	else
	{
		dynamic_string_n_append(table_type, row[1], lengths[1]);
		dynamic_string_append_char(table_type, '\0');
	}
	/* if ((!strcmp(table_type->data,"MRG_MyISAM") || !strcmp(table_type->data,"MRG_ISAM")
		|| !strcmp(table_type->data,"FEDERATED") || !strcmp(table_type->data,"VIEW")))
	*/
	if (dynamic_hash_haskey(ignore_data_types, table_type->data, table_type->len-1) == MC_TRUE)
		ret = MC_TRUE;
	dynamic_string_destroy(table_type);
	end:
	dynamic_string_destroy(sql);
	mysql_free_result(results);
	return ret;
}

static int is_ignore_database(const char *database)
{
	if ((databases_re !=NULL && mc_pcre_exec(databases_re, database, strlen(database))<0) \
		|| (ignore_databases != NULL && dynamic_hash_haskey(ignore_databases, database, strlen(database)) == MC_TRUE))
		return MC_TRUE;
	return MC_FALSE;
}

static int dump_master_data_file(const char *filename)
{
	int ret = MC_TRUE;
	MYSQL_RES *results = NULL;
	unsigned long *lengths;
	if (v_mysql_query(mysql, "SHOW MASTER STATUS") != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	row = mysql_fetch_row(results);
	if (row == NULL)
	{
		log_error("Warning: master status is empty\n");
		goto end;
	}
	lengths = mysql_fetch_lengths(results);
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "CHANGE MASTER TO MASTER_LOG_FILE='");
	dynamic_string_n_append(sql, row[0], (size_t)lengths[0]);
	dynamic_string_append(sql, "', MASTER_LOG_POS=");
	dynamic_string_n_append(sql, row[1], (size_t)lengths[1]);
	dynamic_string_append(sql, ";");
	dynamic_string_append_char(sql, '\0');
	if (opt_verbose > 1)
		log_error("%s\n", sql->data);
	if (dump_mysql_form(filename, sql->data, sql->len-1) == MC_FALSE)
	{
			ret = MC_FALSE;
			goto end; 
	}
	end:
	mysql_free_result(results);
	return ret;
}

static int dump_master_data()
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, "master_status.sql");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return dump_master_data_file(dstring->data);

}

static DARRAY * get_dump_databases(DARRAY *databases)
{
	MYSQL_RES *results = NULL;
	unsigned long *lengths;
	char *database;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW DATABASES");
	if (databases_where != NULL)
	{
		dynamic_string_append(sql, " WHERE ");
		dynamic_string_append(sql, databases_where);
	}
	dynamic_string_append_char(sql, '\0');

	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return NULL;
	}
   	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return NULL;
	}
   	while((row = mysql_fetch_row(results)))
	{
		lengths = mysql_fetch_lengths(results);
		database = string_ncopy(row[0], lengths[0]);
		dynamic_array_push(databases, database);
   	}
	mysql_free_result(results);
	return databases;
}

static DARRAY * get_dump_tables(DARRAY *tables)
{
	MYSQL_RES *results = NULL;
	unsigned long *lengths;
	char *table;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW TABLES");
	if (tables_where != NULL)
	{
	        dynamic_string_append(sql, " WHERE ");
	        dynamic_string_append(sql, tables_where);
	}
	dynamic_string_append_char(sql, '\0');
	
	if (v_mysql_query(mysql, sql->data) != 0)
	{
	        log_error("Error: can't query: %s\n", mysql_error(mysql));
	        return NULL;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return NULL;
	}
	while((row = mysql_fetch_row(results)))
	{
	        lengths = mysql_fetch_lengths(results);
	        table = string_ncopy(row[0], lengths[0]);
	        dynamic_array_push(tables, (void *)table);
	}
	mysql_free_result(results);
	return tables;
}

static DARRAY * get_dump_procedures(DARRAY *procedures)
{
	MYSQL_RES *results = NULL;
	unsigned long *lengths;
	char *procedure;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW PROCEDURE STATUS");
	dynamic_string_append_char(sql, '\0');
	
	if (v_mysql_query(mysql, sql->data) != 0)
	{
	        log_error("Error: can't query: %s\n", mysql_error(mysql));
	        return NULL;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return NULL;
	}
	while((row = mysql_fetch_row(results)))
	{
	        lengths = mysql_fetch_lengths(results);
	        procedure = string_ncopy(row[1], lengths[1]);
	        dynamic_array_push(procedures, (void *)procedure);
	}
	mysql_free_result(results);
	return procedures;
}

static DARRAY * get_dump_triggers(DARRAY *triggers)
{
	MYSQL_RES *results = NULL;
	unsigned long *lengths;
	char *trigger;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW TRIGGERS");
	dynamic_string_append_char(sql, '\0');
	
	if (v_mysql_query(mysql, sql->data) != 0)
	{
	        log_error("Error: can't query: %s\n", mysql_error(mysql));
	        return NULL;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return NULL;
	}
	while((row = mysql_fetch_row(results)))
	{
	        lengths = mysql_fetch_lengths(results);
	        trigger = string_ncopy(row[0], lengths[0]);
	        dynamic_array_push(triggers, (void *)trigger);
	}
	mysql_free_result(results);
	return triggers;
}

static DARRAY * get_dump_functions(DARRAY *functions)
{
	MYSQL_RES *results = NULL;
	unsigned long *lengths;
	char *function;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW FUNCTION STATUS");
	dynamic_string_append_char(sql, '\0');
	
	if (v_mysql_query(mysql, sql->data) != 0)
	{
	        log_error("Error: can't query: %s\n", mysql_error(mysql));
	        return NULL;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return NULL;
	}
	while((row = mysql_fetch_row(results)))
	{
	        lengths = mysql_fetch_lengths(results);
	        function = string_ncopy(row[0], lengths[0]);
	        dynamic_array_push(functions, (void *)function);
	}
	mysql_free_result(results);
	return functions;
}

static int cmpstringgp(const void *p1, const void *p2)
{
	return strcmp(*(char * const *)p1, *(char * const *)p2);
}

/*
	Get the prefixes of array
	SYNOPSIS
		array		array of modify
		direcotry	direcotry path
		type		Extension name of file
			0:
				.frm
			1:
				.func
			2:
				.proc
	RETURN
		pointer to the prefixes of array
		NULL if error
*/

static DARRAY * get_import_prefixes(DARRAY *array, const char *directory, int type)
{
	struct stat s;
	DIR *dir_p;
	struct dirent *dir;
	const char *p;
	const char *s1 = ".frm";
	const char *s2 = ".frm.gz";
	if (type == 1)
	{
		s1 = ".func";
		s2 = ".func.gz";
	}
	else
	if (type == 2)
	{
		s1 = ".proc";
		s2 = ".proc.gz";
	}
	else
	if (type == 3)
	{
		s1 = ".trigger";
		s2 = ".trigger.gz";
	}
	if ((dir_p = opendir(directory)) == NULL)
	{
		log_error("Error: can't opendir \"%s\": %s\n", directory, strerror(errno));
		return NULL;
	}
	while((dir = readdir(dir_p)) != NULL)
	{
		dynamic_string_reset(dstring);
		dynamic_string_append(dstring, directory);
		dynamic_string_append(dstring, "/");
		dynamic_string_append(dstring, dir->d_name);
		dynamic_string_append_char(dstring, '\0');
		if (stat(dstring->data, &s) == -1)
			continue;
		if (!S_ISREG(s.st_mode) || s.st_size == 0)
			continue;
		if (opt_gzip)
		{
			if (strlen(dir->d_name)<=strlen(s2)) continue;
			if (strcmp(dir->d_name+strlen(dir->d_name)-strlen(s2), s2) != 0)
				continue;
			p = string_ncopy(dir->d_name, strlen(dir->d_name)-strlen(s2));
		} 
		else
		{
			if (strlen(dir->d_name)<=strlen(s1)) continue;
			if (strcmp(dir->d_name+strlen(dir->d_name)-strlen(s1), s1) != 0)
				continue;
			p = string_ncopy(dir->d_name, strlen(dir->d_name)-strlen(s1));
		}
		dynamic_array_push(array, (void *)p);
	}
	if (errno == EBADF)
	{
		log_error("Error: readdir error: %s\n", strerror(errno));
		return NULL;
	}
	qsort(array->array, dynamic_array_count(array), sizeof(char *), cmpstringgp);
	if (opt_debug)
	{
		size_t i;
		log_error("Find prefixes:\n");
		for (i=0; i<dynamic_array_count(array); i++)
		{
			log_error("%lu  => %s\n", i, (char *)dynamic_array_fetch(array, i));
		}
	}
	closedir(dir_p);
	return array;
}

static DARRAY *get_import_databases(DARRAY *databases, const char *directory)
{
	return get_import_prefixes(databases, directory, 0);
}

static DARRAY *get_import_tables(DARRAY *tables, const char *directory)
{
	return get_import_prefixes(tables, directory, 0);
}

static DARRAY *get_import_functions(DARRAY *functions, const char *directory)
{
	return get_import_prefixes(functions, directory, 1);
}

static DARRAY *get_import_procedures(DARRAY *procedures, const char *directory)
{
	return get_import_prefixes(procedures, directory, 2);
}

static DARRAY *get_import_triggers(DARRAY *triggers, const char *directory)
{
	return get_import_prefixes(triggers, directory, 2);
}

static int use_database(const char *database)
{
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "USE `");
	dynamic_string_append(sql, database);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');
	
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	/* @@character_set_database == character set for import csv file == dump default_character_set */
	
	//dynamic_string_reset(sql);
	//dynamic_string_append(sql, "/*!40101 set @@character_set_database=");
	//dynamic_string_append(sql, opt_default_character_set);
	//dynamic_string_append(sql, " */;");
	//dynamic_string_append_char(sql, '\0');
	//if (v_mysql_query(mysql, sql->data) != 0)
	//{
	//	log_error("Error: can't query: %s\n", mysql_error(mysql));
	//	return MC_FALSE;
	//}
	return MC_TRUE;
}

static int dump_function_file(const char *function, const char *filename)
{
	MYSQL_RES *results = NULL;
	MYSQL_ROW row;
	unsigned long *lengths;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW CREATE FUNCTION `");
	dynamic_string_append(sql, function);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');

	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	row = mysql_fetch_row(results);
	lengths = mysql_fetch_lengths(results);
	if (dump_mysql_form(filename, row[2], lengths[2]) == MC_FALSE)
	{
		mysql_free_result(results);
		return MC_FALSE;
	}
	mysql_free_result(results);
	return MC_TRUE;
}

static int dump_function(const char *function)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, function);
	dynamic_string_append(dstring, ".func");
	if (opt_gzip){
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return dump_function_file(function, dstring->data);
}

static int dump_procedure_file(const char *procedure, const char *filename)
{
	MYSQL_RES *results = NULL;
	MYSQL_ROW row;
	unsigned long *lengths;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW CREATE PROCEDURE `");
	dynamic_string_append(sql, procedure);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');

	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	row = mysql_fetch_row(results);
	lengths = mysql_fetch_lengths(results);
	if (dump_mysql_form(filename, row[2], lengths[2]) == MC_FALSE)
	{
		mysql_free_result(results);
		return MC_FALSE;
	}
	mysql_free_result(results);
	return MC_TRUE;
}

static int dump_procedure(const char *procedure)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, procedure);
	dynamic_string_append(dstring, ".proc");
	if (opt_gzip)
		dynamic_string_append(dstring, ".gz");
	dynamic_string_append_char(dstring, '\0');
	return dump_procedure_file(procedure, dstring->data);
}

static int dump_trigger_file(const char *trigger, const char *filename)
{
	MYSQL_RES *results = NULL;
	MYSQL_ROW row;
	unsigned long *lengths;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW CREATE TRIGGER `");
	dynamic_string_append(sql, trigger);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');

	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	row = mysql_fetch_row(results);
	lengths = mysql_fetch_lengths(results);
	if (dump_mysql_form(filename, row[2], lengths[2]) == MC_FALSE)
	{
		mysql_free_result(results);
		return MC_FALSE;
	}
	mysql_free_result(results);
	return MC_TRUE;
}

static int dump_trigger(const char *trigger)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, trigger);
	dynamic_string_append(dstring, ".trigger");
	if (opt_gzip)
		dynamic_string_append(dstring, ".gz");
	dynamic_string_append_char(dstring, '\0');
	return dump_trigger_file(trigger, dstring->data);
}

static int dump_table_form_file(const char *table, const char *filename)
{
	MYSQL_RES *results = NULL;
	MYSQL_ROW row;
	unsigned long *lengths;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW CREATE TABLE `");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');

	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	row = mysql_fetch_row(results);
	//unsigned int num_fields = mysql_num_fields(results);
	lengths = mysql_fetch_lengths(results);
	//printf("%lu %.*s\n", lengths[0], (size_t)lengths[0], row[0]);
	//printf("%lu %.*s\n", lengths[1], (size_t)lengths[1], row[1]);
	if (dump_mysql_form(filename, row[1], lengths[1]) == MC_FALSE)
	{
		mysql_free_result(results);
		return MC_FALSE;
	}
	mysql_free_result(results);
	return MC_TRUE;
}


static int dump_table_form(const char *table)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, table);
	dynamic_string_append(dstring, ".frm");
	if (opt_gzip)
		dynamic_string_append(dstring, ".gz");
	dynamic_string_append_char(dstring, '\0');
	return dump_table_form_file(table, dstring->data);
}
/*
	RETURN:
		MC_OK		0	Success
		MC_FAILURE	-1  Failure
		MC_IGNORE	1	Ignore content
*/
static int import_table_form_file(const char *table, const char *filename)
{
	const char *s = "IF NOT EXISTS ";
	DSTRING *drop_sql;
	dynamic_string_reset(sql);
	int rc;
	if (opt_gzip)
	{
		if (dynamic_string_gzreadfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	else
	{
		if (dynamic_string_readfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	if (mc_pcre_exec(tables_form_re, sql->data, sql->len)<0)
		return MC_IGNORE;
	drop_sql = dynamic_string_new(128);
	if (opt_drop_table)
		dynamic_string_append(drop_sql, "DROP ");
	rc = mc_pcre_exec(tables_form_replace_re, sql->data, sql->len);
	if (rc>=0)
	{
		if (opt_drop_table)
			dynamic_string_append(drop_sql, "TABLE");
		dynamic_string_insert(sql, ovector[2*0+1], s); 
	}
	else
	{
		if (opt_drop_table)
			dynamic_string_append(drop_sql, "VIEW");
	}
	if (opt_drop_table)
	{
		dynamic_string_append(drop_sql, " IF EXISTS `");
		dynamic_string_append(drop_sql, table);
		dynamic_string_append(drop_sql, "`");
		dynamic_string_append_char(drop_sql, '\0');
		if (v_mysql_query(mysql, drop_sql->data) != 0)
		{
			log_error("Error: can't query: %s\n", mysql_error(mysql));
			return MC_FAILURE;
		}
	}
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FAILURE;
	}
	return MC_OK;
}



/*
	RETURN:
		MC_OK		0	Success
		MC_FAILURE	-1  Failure
		MC_IGNORE	1	Ignore content
*/
static int import_func_file(const char *table, const char *filename)
{
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "DROP FUCTION IF EXISTS `");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FAILURE;
	}
	dynamic_string_reset(sql);
	if (opt_gzip)
	{
		if (dynamic_string_gzreadfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	else
	{
		if (dynamic_string_readfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	if (functions_re != NULL && mc_pcre_exec(functions_re, sql->data, sql->len)<0)
	{
		return MC_IGNORE;
	}
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FAILURE;
	}
	return MC_OK;
}

/*
	RETURN:
		MC_OK		0	Success
		MC_FAILURE	-1  Failure
		MC_IGNORE	1	Ignore content
*/
static int import_proc_file(const char *table, const char *filename)
{
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "DROP PROCEDURE IF EXISTS `");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FAILURE;
	}
	dynamic_string_reset(sql);
	if (opt_gzip)
	{
		if (dynamic_string_gzreadfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	else
	{
		if (dynamic_string_readfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	if (procedures_re != NULL && mc_pcre_exec(procedures_re, sql->data, sql->len)<0)
	{
		return MC_IGNORE;
	}
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FAILURE;
	}
	return MC_OK;
}

/*
	RETURN:
		MC_OK		0	Success
		MC_FAILURE	-1  Failure
		MC_IGNORE	1	Ignore content
*/
static int import_trigger_file(const char *table, const char *filename)
{
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "DROP TRIGGERS IF EXISTS `");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FAILURE;
	}
	dynamic_string_reset(sql);
	if (opt_gzip)
	{
		if (dynamic_string_gzreadfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	else
	{
		if (dynamic_string_readfile(sql, filename) == MC_FALSE)
			return MC_FAILURE;
	}
	if (triggers_re != NULL && mc_pcre_exec(triggers_re, sql->data, sql->len)<0)
	{
		return MC_IGNORE;
	}
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FAILURE;
	}
	return MC_OK;
}




static int import_table_form(const char *table)
{	
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, table);
	dynamic_string_append(dstring, ".frm");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return import_table_form_file(table, dstring->data);
}


static int import_function(const char *function)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, function);
	dynamic_string_append(dstring, ".func");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return import_func_file(function, dstring->data);
}

static int import_procedure(const char *procedure)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, procedure);
	dynamic_string_append(dstring, ".proc");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return import_proc_file(procedure, dstring->data);
}


static int import_trigger(const char *trigger)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, trigger);
	dynamic_string_append(dstring, ".trigger");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return import_trigger_file(trigger, dstring->data);
}

static int dump_table_file(const char *table, const char *filename)
{
	MYSQL_RES *results = NULL;
	DSTRING *dst = NULL;
	my_ulonglong num_rows;
	unsigned int num_fields, i;
	unsigned long *lengths;
	mc_fh_t fh;
	mc_ff_t ff;
	ff.fh = &fh;
	int ret = MC_TRUE;
	int pipefd[2];
	int child_ret = 0;
	int fd;

	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SELECT /*!40001 SQL_NO_CACHE */ * FROM `");
	dynamic_string_append(sql, table);
	//dynamic_string_append(sql, "` LIMIT 10");
	dynamic_string_append(sql, "`");
	if (where != NULL)
	{
		dynamic_string_append(sql, " WHERE ");
		dynamic_string_append(sql, where);
	}
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	if (opt_quick)
		results = mysql_use_result(mysql);
	else
		results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	num_fields = mysql_num_fields(results);
	if (opt_verbose>1)
		log_error("Open file: '%s'\n", filename);
	if (opt_gzip && opt_pipe)
	{
		fd = open(filename, O_CREAT|O_TRUNC|O_EXCL|O_WRONLY, 0644);
		if (fd == -1)
		{
			log_error("Error: can't open '%s': %s!\n", filename, strerror(errno));
			ret = MC_FALSE;
			goto end;
		}
		signal(SIGINT, sighandler);
		signal(SIGTERM, sighandler);
		signal(SIGPIPE, sighandler);
		if (pipe(pipefd) == -1)
		{
			log_error("Error: pipe() failed: %s\n", strerror(errno));
			ret = MC_FALSE;
			goto end;
		};
		pid = fork();
		if (pid == -1)
		{
			log_error("Error: can't fork(): %s\n", strerror(errno));
			ret = MC_FALSE;
			goto end;
		}
		if (pid == 0)
		{
			char buffer[BUF_SIZE];
			gzFile *gf = NULL;
			int bytes1, bytes2;
			signal(SIGINT, sighandler);
			signal(SIGTERM, sighandler);
			signal(SIGPIPE, SIG_DFL);
			close(pipefd[1]);
			if (child_kill)
				goto child_end;
			char mode[10];
			snprintf(mode, 10, "wb%d", opt_gzip);
			gf = gzdopen(fd, mode);
			if (gf == NULL)
			{
				log_error("Error: can't gzdopen '%s': %s!\n", filename, strerror(errno));
				goto child_end;
			}
			for (;;)
			{
				if (child_kill)
					goto child_end;
				bytes1 = read(pipefd[0], buffer, BUF_SIZE);
				if (bytes1 == 0)
					break;
				if (bytes1 == -1)
				{
					log_error("Error: read() failed: %s\n", strerror(errno));
					child_ret = 1;
					goto child_end;
				}
				if (child_kill)
					goto child_end;
				for (;bytes1>0;bytes1-=bytes2)
				{
					bytes2 = gzwrite(gf, buffer, bytes1);
					if (bytes2 == 0)
					{
						log_error("Error: gzwrite() failed: %s\n", strerror(errno));
						child_ret = 1;
						goto child_end;
					}
					if (child_kill)
						goto child_end;
				}
			}
			child_end:
			if (gf != NULL)
				gzclose(gf);
			close(pipefd[1]);
			exit(child_ret);
		}
		else
		{
			close(pipefd[0]);
			fh.fd = pipefd[1];
			ff.write = mc_fd_write;
			ff.close = mc_fd_close;
		}
	}
	else
	if (mc_file_open(&ff, filename) == MC_FALSE)
	{
		ret = MC_FALSE;
		goto end;
	}
	dst = dynamic_string_new(128);

	while((row = mysql_fetch_row(results)))
	{
		lengths = mysql_fetch_lengths(results);
		dynamic_string_reset(dst);
		if (child_kill)
		{
			ret = MC_FALSE;
			goto end;
		}
		for (i = 0; i < num_fields; i++)
		{
			if (child_kill)
			{
				ret = MC_FALSE;
				goto end;
			}
			//printf("here1 dst->len=%d lengths[i]=%d dst->size=%d 1000*BUF_SIZE\n", dst->len, lengths[i], dst->size, 1000 * BUF_SIZE);
			/* To prevent excessive of dynamic string buffer size */
			if (dst->len && dst->len + lengths[i] > dst->size && dst->len + lengths[i] > 1000 * BUF_SIZE)
			{
				if (child_kill)
				{
					ret = MC_FALSE;
					goto end;
				}
				if (ff.write(ff.fh, dst->data, dst->len) == MC_FALSE)
				{
					ret = MC_FALSE;
					goto end;
				}
				if (child_kill)
				{
					ret = MC_FALSE;
					goto end;
				}
				dynamic_string_reset(dst);
			}
			if (row[i] == NULL)
			{
				dynamic_string_append(dst, "\\N");
			}
			else
			{
				dynamic_string_append_csv_field(dst, row[i], lengths[i]);
			}
			if (i != num_fields - 1)
				dynamic_string_append_char(dst, '\t');
		}
		dynamic_string_append_char(dst, '\n');
		if (child_kill)
		{
			ret = MC_FALSE;
			goto end;
		}
		if (ff.write(ff.fh, dst->data, dst->len) == MC_FALSE)
		{
			ret = MC_FALSE;
			goto end;
		}
		if (child_kill)
		{
			ret = MC_FALSE;
			goto end;
		}
	}
	if (opt_verbose)
	{
		num_rows = mysql_num_rows(results);
		log_error("Query OK: %lu rows\n", num_rows); 
	}
	end:
	dynamic_string_destroy(dst);
	ff.close(ff.fh);
	if (opt_verbose>1)
		log_error("Close file\n");
	if (opt_gzip && opt_pipe)
	{
		int status;
		if (!child_kill && ret == MC_FALSE)
		{
			kill(pid, SIGPIPE);
		}
		waitpid(pid, &status, 0);
		if (opt_verbose)
			log_error("Child exit with %d\n", WEXITSTATUS(status));
		signal(SIGINT, SIG_DFL);
		signal(SIGTERM, SIG_DFL);
		signal(SIGPIPE, SIG_DFL);
		if (child_kill)
		{
			log_error("User killed\n");
			exit(1);
		}
	}
	mysql_free_result(results);
	return ret;
}


static int dump_table(const char *table)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, table);
	dynamic_string_append(dstring, ".csv");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return dump_table_file(table, dstring->data);
}





static int is_exists_table_data(const char *table)
{
	struct stat s;
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, table);
	dynamic_string_append(dstring, ".csv");
	if (opt_gzip)
		dynamic_string_append(dstring, ".gz");
	dynamic_string_append_char(dstring, '\0');
	if (stat(dstring->data, &s) == -1)
	{
		return MC_FALSE;
	}
	return MC_TRUE;
	
}

static int import_table_file(const char *table, const char *filename)
{

	//MYSQL_RES *results = NULL;
	int ret = MC_TRUE;
	my_ulonglong num_rows;
	unsigned int warning_count;
	mc_fh_t fh;
	mc_ff_t ff;
	//int pid = -1;
	int status;
	int i;
	ff.fh = &fh;
    if (opt_disable_keys)
	{
    	dynamic_string_reset(sql);
	    dynamic_string_append(sql, "ALTER TABLE `");
	    dynamic_string_append(sql, table);
	    dynamic_string_append(sql, "` DISABLE KEYS");
	    dynamic_string_append_char(sql, '\0');

	    if (v_mysql_query(mysql, sql->data) != 0)
		{
	    	log_error("Error: can't query: %s\n", mysql_error(mysql));
            return MC_FALSE;
	    }
    }

	if (opt_gzip)
	{
		i = 0;
		if (fifo_file == NULL)
		{
			fifo_file = dynamic_string_new(128);
			dynamic_string_append(fifo_file, opt_tmpdir);
			dynamic_string_append(fifo_file, "/.mysqlcsv");
			dynamic_string_append(fifo_file, ".fifo-XXXXXX");
			dynamic_string_append_char(fifo_file, '\0');
			while (1)
			{
				if (mktemp(fifo_file->data) == NULL)
				{
					log_error("Error: mktemp(): %s\n", strerror(errno));
					return MC_FALSE;
				}
				if (opt_verbose>1)
					log_error("mkfifo: %s\n", fifo_file->data);
				if (mkfifo(fifo_file->data, O_CREAT|O_EXCL|O_RDWR|O_NONBLOCK) == -1)
				{
					log_error("Error: mkfifo(): %s\n", strerror(errno));
					if (i++<3)
						continue;
					return MC_FALSE;
				}
				//chmod(fifo_file->data, 0777);
				break;
			}
		}
		signal(SIGINT, sighandler);
		signal(SIGTERM, sighandler);
		signal(SIGPIPE, sighandler);
		pid = fork();
		if (pid == -1)
		{
			log_error("Error: fork(): %s\n", strerror(errno));
			return MC_FALSE;
		}
		if (pid == 0)
		{
			int fd;
			ssize_t bytes;
			char buffer[BUF_SIZE];
			gzFile gzfile;
			int gzerrno;
			int child_ret = 0;
			signal(SIGINT, sighandler);
			signal(SIGTERM, sighandler);
			signal(SIGPIPE, SIG_DFL);
			if (child_kill)
				exit(1);
			if ((fd = open(fifo_file->data, O_WRONLY)) == -1)
			{
				log_error("Error: open fifo '%s': %s\n", fifo_file->data, strerror(errno));
				exit(1);
			}
			if (child_kill)
			{
				child_ret = 1;
				goto end_child2;
			}
			if ((gzfile = gzopen(filename, "rb")) == NULL)
			{
				log_error("Error: gzopen '%s': %s\n", filename, strerror(errno));
				child_ret = 1;
				goto end_child2;
			}
			if (child_kill)
			{
				child_ret = 1;
				goto end_child2;
			}
			while ((bytes = gzread(gzfile, buffer, BUF_SIZE)) > 0)
			{
				if (child_kill)
				{
					child_ret = 1; 
					goto end_child1;
				}
				//printf("%.*s\n", bytes, buffer);
				if (fd_write(fd, buffer, bytes) == MC_FALSE)
					goto end_child1;
			}
			if (bytes == -1)
			{
				log_error("Error: gzread: %s\n", gzerror(gzfile, &gzerrno));
				child_ret = 1;
			}
			end_child1:
			gzclose(gzfile);
			end_child2:
			close(fd);
			exit(child_ret);
		}
	}
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "LOAD DATA LOCAL INFILE '");
	if (opt_gzip)
		dynamic_string_append(sql,  fifo_file->data);
	else
		dynamic_string_append(sql,  filename);
	dynamic_string_append(sql, "'");
	if (opt_ignore)
	{
		dynamic_string_append(sql, " IGNORE");
	}
	if (opt_replace)
	{
		dynamic_string_append(sql, " REPLACE");
	}
	dynamic_string_append(sql, " INTO TABLE `");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "`");
	if (opt_default_character_set != NULL && ((server_version >= 50038L && server_version <= 50100L) || server_version >= 50117L))
	{
		dynamic_string_append(sql, " CHARACTER SET '");
		dynamic_string_append(sql, opt_default_character_set);
		dynamic_string_append(sql, "'");
	}
	dynamic_string_append_char(sql, '\0');
	if (child_kill)
	{
		ret = MC_FALSE;
		goto end;
	}
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		ret = MC_FALSE;
		goto end;
	}
	if (opt_verbose)
	{
		warning_count = mysql_warning_count(mysql);
		num_rows = mysql_affected_rows(mysql);
		log_error("Query OK: %lu rows affected, %lu rows warning\n", num_rows, warning_count); 
	}
	end:	
	if (opt_gzip)
	{
		if (!child_kill && ret == MC_FALSE)
		{
			kill(pid, SIGPIPE);
		}
		waitpid(pid, &status, 0);
		if (opt_verbose)
			log_error("Child exit with %d\n", WEXITSTATUS(status));
		if (ret == MC_TRUE && status != 0)
		{
			ret = MC_FALSE;
		}
		signal(SIGINT, SIG_DFL);
		signal(SIGTERM, SIG_DFL);
		signal(SIGPIPE, SIG_DFL);
		if (child_kill)
		{
			log_error("User killed\n");
			if (fifo_file != NULL)
			{
				unlink(fifo_file->data);
				dynamic_string_destroy(fifo_file);
				fifo_file = NULL;
			}
			exit(1);
		}
	}

    if (opt_disable_keys)
	{
    	dynamic_string_reset(sql);
	    dynamic_string_append(sql, "ALTER TABLE `");
	    dynamic_string_append(sql, table);
	    dynamic_string_append(sql, "` ENABLE KEYS");
	    dynamic_string_append_char(sql, '\0');
	    if (v_mysql_query(mysql, sql->data) != 0)
		{
	    	log_error("Error: can't query: %s\n", mysql_error(mysql));
            return MC_FALSE;
	    }
    }

	/*
    if (opt_disable_keys)
	{
    	dynamic_string_reset(sql);
	    dynamic_string_append(sql, "REPAIR TABLE `");
	    dynamic_string_append(sql, table);
	    dynamic_string_append(sql, "`");
	    dynamic_string_append_char(sql, '\0');
	    if (v_mysql_query(mysql, sql->data) != 0)
		{
		    log_error("Error: can't query: %s\n", mysql_error(mysql));
            return MC_FALSE;
    	}
	    results = mysql_store_result(mysql);
	    if (results == NULL)
		{
	    	log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
	    	return MC_FALSE;
	    }
	    mysql_free_result(results);
    }
	*/

	return ret;
}


static int import_table(const char *table)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, table);
	if (opt_gzip)
		dynamic_string_append(dstring, ".csv.gz");
	else
		dynamic_string_append(dstring, ".csv");
	dynamic_string_append_char(dstring, '\0');
	return import_table_file(table, dstring->data);
}

static int dump_database_form_file(const char *database, const char *filename)
{
	MYSQL_RES *results = NULL;
	MYSQL_ROW row;
	unsigned long *lengths;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "SHOW CREATE DATABASE `");
	dynamic_string_append(sql, database);
	dynamic_string_append(sql, "`");
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	results = mysql_store_result(mysql);
	if (results == NULL)
	{
		log_error("Error: mysql failed to store result: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	row = mysql_fetch_row(results);
	lengths = mysql_fetch_lengths(results);
	if (dump_mysql_form(filename, row[1], lengths[1]) == MC_FALSE)
	{
		mysql_free_result(results);
		return MC_FALSE;
	}
	mysql_free_result(results);
	return MC_TRUE;
}

static int dump_database_form(const char *database)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, database);
	dynamic_string_append(dstring, ".frm");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return dump_database_form_file(database, dstring->data);

}

static int import_database_form_file(const char *database, const char *filename)
{
	const char *s1 = "CREATE DATABASE ";
	const char *s2 = "IF NOT EXISTS ";
	dynamic_string_reset(sql);
	if (opt_gzip)
	{
		if (dynamic_string_gzreadfile(sql, filename) == MC_FALSE)
			return MC_FALSE;
	}
	else
	{
		if (dynamic_string_readfile(sql, filename) == MC_FALSE)
			return MC_FALSE;
	}
	if (strncmp(s1, sql->data, strlen(s1)) != 0)
	{
		log_error("Error: database form is not match: %s", sql->data);
		return MC_FALSE;
	}
	
	dynamic_string_append_char(sql, '\0');
	if (sql->len + strlen(s2) > sql->size)
	{
		dynamic_string_resize(sql, sql->len+strlen(s2));
	}
	memmove(sql->data+strlen(s1)+strlen(s2), sql->data+strlen(s1), sizeof(char) * (sql->len-strlen(s1)));
	memcpy(sql->data+strlen(s1), s2, strlen(s2));
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	return MC_TRUE;
}

static int import_database_form(const char *database)
{
	dynamic_string_reset(dstring);
	dynamic_string_append(dstring, database);
	dynamic_string_append(dstring, ".frm");
	if (opt_gzip)
	{
		dynamic_string_append(dstring, ".gz");
	}
	dynamic_string_append_char(dstring, '\0');
	return import_database_form_file(database, dstring->data);
}

static int lock_table_write(const char *table)
{
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "LOCK TABLES `");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "` write");
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	return MC_TRUE;
}

/*
static int lock_tables_read(DARRAY *tables, const char *database)
{
	size_t i;
	char *table;
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "LOCK TABLES");
	for (i=0; i<dynamic_array_count(tables); i++)
	{
		table = dynamic_array_fetch(tables, i);
		if (is_ignore_table(table, database) == MC_TRUE)
			continue;
		if (is_ignore_table_data(table, database) == MC_TRUE)
			continue;
		if (i>0)
			dynamic_string_append(sql, ", ");
		dynamic_string_append(sql, " `");
		dynamic_string_append(sql, table);
		dynamic_string_append(sql, "` READ");
	}
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	return MC_TRUE;
}
*/

static int lock_table_read(const char *table)
{
	dynamic_string_reset(sql);
	dynamic_string_append(sql, "LOCK TABLES `");
	dynamic_string_append(sql, table);
	dynamic_string_append(sql, "` READ");
	dynamic_string_append_char(sql, '\0');
	if (v_mysql_query(mysql, sql->data) != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	return MC_TRUE;
}

static int unlock_tables()
{
	if (v_mysql_query(mysql, "UNLOCK TABLES") != 0)
	{
		log_error("Error: can't query: %s\n", mysql_error(mysql));
		return MC_FALSE;
	}
	return MC_TRUE;
}

static int dump_all_functions()
{	
	int ret = MC_TRUE;
	size_t i;
	char *function;
	DARRAY *functions = NULL;
	size_t nd=0, en=0;
	functions = dynamic_array_new();
	if (get_dump_functions(functions) == NULL)
		return MC_FALSE;
	for (i=0; i<dynamic_array_count(functions); i++)
	{
		function = (char *)dynamic_array_fetch(functions, i);
		nd++;
		if (opt_verbose)
			log_error("Dump FUCTION: `%s`\n", function);
		if (dump_function(function) == MC_FALSE)
		{
			en++;
			if (!opt_force) break;
			continue;
		}
	}
	if (!opt_force && en)
		ret = MC_FALSE;
	if (opt_verbose)
		log_error("%ld functions, %ld was failed\n", nd, en);
	for (i=0; i<dynamic_array_count(functions); i++)
	{
		function = (char *)dynamic_array_fetch(functions, i);
		free(function);
	}
	dynamic_array_destroy(functions);	
	return ret;
}

static int dump_all_procedures()
{	
	int ret = MC_TRUE;
	size_t i;
	char *procedure;
	DARRAY *procedures = NULL;
	size_t nd=0, en=0;
	procedures = dynamic_array_new();
	if (get_dump_procedures(procedures) == NULL)
		return MC_FALSE;
	for (i=0; i<dynamic_array_count(procedures); i++)
	{
		procedure = (char *)dynamic_array_fetch(procedures, i);
		nd++;
		if (opt_verbose)
			log_error("Dump PROCEDURE: `%s`\n", procedure);
		if (dump_procedure(procedure) == MC_FALSE)
		{
			en++;
			if (!opt_force) break;
			continue;
		}
	}
	if (!opt_force && (en))
		ret = MC_FALSE;
	if (opt_verbose)
		log_error("%ld procedures, %ld was failed\n", nd, en);
	for (i=0; i<dynamic_array_count(procedures); i++)
	{
		procedure = (char *)dynamic_array_fetch(procedures, i);
		free(procedure);
	}
	dynamic_array_destroy(procedures);	
	return ret;
}

static int dump_all_triggers()
{	
	int ret = MC_TRUE;
	size_t i;
	char *trigger;
	DARRAY *triggers = NULL;
	size_t nd=0, en=0;
	triggers = dynamic_array_new();
	if (get_dump_triggers(triggers) == NULL)
		return MC_FALSE;
	for (i=0; i<dynamic_array_count(triggers); i++)
	{
		trigger = (char *)dynamic_array_fetch(triggers, i);
		nd++;
		if (opt_verbose)
			log_error("Dump TRIGGER: `%s`\n", trigger);
		if (dump_trigger(trigger) == MC_FALSE)
		{
			en++;
			if (!opt_force) break;
			continue;
		}
	}
	if (!opt_force && (en))
		ret = MC_FALSE;
	if (opt_verbose)
		log_error("%ld triggers, %ld was failed\n", nd, en);
	for (i=0; i<dynamic_array_count(triggers); i++)
	{
		trigger = (char *)dynamic_array_fetch(triggers, i);
		free(trigger);
	}
	dynamic_array_destroy(triggers);	
	return ret;
}

static int dump_tables(const char *database, DARRAY *select_tables)
{	
	int ret = MC_TRUE;
	size_t i;
	char *table;
	DARRAY *tables = select_tables;
	size_t nd=0, ef=0, ed=0;
	if (tables == NULL)
	{
		tables = dynamic_array_new();
		if (get_dump_tables(tables) == NULL)
		{
			ret = MC_FALSE;
			goto end;
		}
	}
	for (i=0; i<dynamic_array_count(tables); i++)
	{
		table = (char *)dynamic_array_fetch(tables, i);
		if (is_ignore_table(table, database) == MC_TRUE)
			continue;
		nd++;
		if (opt_lock)
		{
			if (lock_table_read(table) == MC_FALSE)
			{
				ret = MC_FALSE;
				goto endloop;
			}
		}
		if (opt_verbose)
			log_error("Dump table: `%s`\n", table);
		if (dump_table_form(table) == MC_FALSE)
		{
			ef++;
			ret = MC_FALSE;
			goto unlock;
		}
		 if (opt_no_data || is_ignore_table_data(table, database))
			goto unlock;
		if (dump_table(table) == MC_FALSE)
		{
			ed++;
			ret = MC_FALSE;
			goto unlock;
		}
		unlock:
		if (opt_lock)
			unlock_tables();
		endloop:
		if (!opt_force && ret != MC_TRUE)
			break;
	}
	if (ef || ed)
		ret = MC_FALSE;
	if (opt_verbose)
		log_error("%ld tables, %ld was failed of form, %ld was failed of data, %ld was failed\n", nd, ef, ed, ef+ed);
	if (select_tables == NULL)
	{
		for (i=0; i<dynamic_array_count(tables); i++)
		{
			table = (char *)dynamic_array_fetch(tables, i);
			free(table);
		}
	}
	end:
	if (select_tables == NULL)
		dynamic_array_destroy(tables);
	return ret;
}

static int import_tables(const char *database, DARRAY *select_tables)
{	
	size_t i;
	char *table;
	size_t nd=0, ef=0, ed=0;
	int ret = MC_TRUE, r;
	DARRAY *tables = select_tables;
	if (tables == NULL)
	{
		tables = dynamic_array_new();
		if (get_import_tables(tables, ".") == NULL)
		{
			ret = MC_FALSE;
			goto end;
		}
	}

	for (i=0; i<dynamic_array_count(tables); i++)
	{
		table = (char *)dynamic_array_fetch(tables, i);
		if (is_ignore_table(table, database) == MC_TRUE)
			continue;
		r = import_table_form(table);
		if (r == MC_IGNORE)
			continue;
		nd++;
		if (opt_verbose)
			log_error("Import table: `%s`\n", table);
		if (r == MC_FAILURE)
		{
			ef++;
			ret = MC_FALSE;
			goto endloop;
		} 
		if (opt_no_data || is_ignore_table_data(table, database) || !is_exists_table_data(table))
			continue;
		if (opt_lock)
		{
			if (lock_table_write(table) == MC_FALSE)
			{
				ret = MC_FALSE;
				goto unlock;
			}
		}
		if (import_table(table) == MC_FALSE)
		{
			ed++;
			ret = MC_FALSE;
			goto unlock;
		}
		unlock:
		if (opt_lock)
			unlock_tables();
		endloop:
		if (!opt_force && ret != MC_TRUE)
			break;
	}

	if (ef || ed)
		ret =  MC_FALSE;
	if (opt_verbose)
		log_error("%ld tables, %ld was failed of form, %ld was failed of data, %ld was failed\n", nd, ef, ed, ef+ed);
	if (select_tables == NULL)
	{
		for (i=0; i<dynamic_array_count(tables); i++)
		{
			table = (char *)dynamic_array_fetch(tables, i);
			free(table);
		}
	}
	end:
	if (select_tables == NULL)
		dynamic_array_destroy(tables);
	return ret;
}

static int import_all_functions(const char *database)
{	
	size_t i;
	char *function;
	DARRAY *functions = NULL;
	size_t nd=0, en=0;
	int ret = MC_TRUE, r;
	functions = dynamic_array_new();
	if (get_import_functions(functions, ".") == NULL)
		return MC_FALSE;
	for (i=0; i<dynamic_array_count(functions); i++)
	{
		function = (char *)dynamic_array_fetch(functions, i);
		if (is_ignore_function(function, database) == MC_TRUE)
			continue;
		r = import_function(function);
		if (r == MC_IGNORE)
			continue;
		nd++;
		if (opt_verbose)
			log_error("Import function: `%s`\n", function);
		if (r == MC_FAILURE)
		{
			en++;
			if (!opt_force) break;
			continue;
		} 
	}
	if (en)
		ret =  MC_FALSE;
	if (opt_verbose)
		log_error("%ld functions, %ld was failed\n", nd, en);
	for (i=0; i<dynamic_array_count(functions); i++)
	{
		function = (char *)dynamic_array_fetch(functions, i);
		free(function);
	}
	dynamic_array_destroy(functions);
	return ret;
}

static int import_all_procedures(const char *database)
{	
	size_t i;
	char *procedure;
	DARRAY *procedures = NULL;
	size_t nd=0, en=0;
	int ret = MC_TRUE, r;
	procedures = dynamic_array_new();
	if (get_import_procedures(procedures, ".") == NULL)
		return MC_FALSE;
	for (i=0; i<dynamic_array_count(procedures); i++)
	{
		procedure = (char *)dynamic_array_fetch(procedures, i);
		if (is_ignore_procedure(procedure, database) == MC_TRUE)
			continue;
		r = import_procedure(procedure);
		if (r == MC_IGNORE)
			continue;
		nd++;
		if (opt_verbose)
			log_error("Import procedure: `%s`\n", procedure);
		if (r == MC_FAILURE)
		{
			en++;
			if (!opt_force) break;
			continue;
		} 
	}
	if (en)
		ret =  MC_FALSE;
	if (opt_verbose)
		log_error("%ld procedures, %ld was failed\n", nd, en);
	for (i=0; i<dynamic_array_count(procedures); i++)
	{
		procedure = (char *)dynamic_array_fetch(procedures, i);
		free(procedure);
	}
	dynamic_array_destroy(procedures);
	return ret;
}

static int import_all_triggers(const char *database)
{	
	size_t i;
	char *trigger;
	DARRAY *triggers = NULL;
	size_t nd=0, en=0;
	int ret = MC_TRUE, r;
	triggers = dynamic_array_new();
	if (get_import_triggers(triggers, ".") == NULL)
		return MC_FALSE;
	for (i=0; i<dynamic_array_count(triggers); i++)
	{
		trigger = (char *)dynamic_array_fetch(triggers, i);
		if (is_ignore_trigger(trigger, database) == MC_TRUE)
			continue;
		r = import_trigger(trigger);
		if (r == MC_IGNORE)
			continue;
		nd++;
		if (opt_verbose)
			log_error("Import trigger: `%s`\n", trigger);
		if (r == MC_FAILURE)
		{
			en++;
			if (!opt_force) break;
			continue;
		} 
	}
	if (en)
		ret =  MC_FALSE;
	if (opt_verbose)
		log_error("%ld triggers, %ld was failed\n", nd, en);
	for (i=0; i<dynamic_array_count(triggers); i++)
	{
		trigger = (char *)dynamic_array_fetch(triggers, i);
		free(trigger);
	}
	dynamic_array_destroy(triggers);
	return ret;
}

static int dump_database(const char *database, DARRAY *tables)
{
	struct stat s;
	int ret = MC_TRUE;
	if (use_database(database) == MC_FALSE)
		return MC_FALSE;
	if (stat(database, &s) == -1)
	{
		if (opt_verbose>1)
			log_error("Mkdir: '%s'\n", database);
		if (mkdir(database, 0755) == -1)
		{
			log_error("Error: can't mkdir: %s\n", strerror(errno));
			return MC_FALSE;
		}
	}
	if (opt_verbose>1)
		log_error("Chdir: '%s'\n", database);
	if (chdir(database) == -1)
	{
		log_error("Error: can't chdir: %s\n", strerror(errno));
		return MC_FALSE;
	}
	
	if (dump_tables(database, tables) == MC_FALSE)
	{
		ret = MC_FALSE;
		if (!opt_force)
			goto end;
	}
	if (opt_function)
	{
		if (dump_all_functions(database) == MC_FALSE)
		{
			ret = MC_FALSE;
			if (!opt_force)
				goto end;
		}
	}
	if (opt_procedure)
	{
		if (dump_all_procedures(database) == MC_FALSE)
		{
			ret = MC_FALSE;
			if (!opt_force)
				goto end;
		}
	}
	if (opt_trigger)
	{
		if (dump_all_triggers(database) == MC_FALSE)
		{
			ret = MC_FALSE;
			if (!opt_force)
				goto end;
		}
	}
	end:
	if (opt_verbose>1)
		log_error("Chdir: '..'\n");
	chdir("..");
	return ret;
}

static int set_database_character()
{
	if (opt_default_character_set != NULL && !((server_version >= 50038L && server_version <= 50100L) || server_version >= 50117L))
	{
		dynamic_string_reset(sql);
		dynamic_string_append(sql, "SET character_set_database='");
		dynamic_string_append(sql, opt_default_character_set);
		dynamic_string_append(sql, "'");
		dynamic_string_append_char(sql, '\0');
		if (v_mysql_query(mysql, sql->data) != 0)
		{
			log_error("Error: can't query: %s\n", mysql_error(mysql));
			return MC_FALSE;
		}
	}
	return MC_TRUE;

}

static int import_database(const char *database, DARRAY *tables)
{

	int ret = MC_TRUE;
	if (use_database(database) == MC_FALSE)
		return MC_FALSE;
	if (set_database_character() == MC_FALSE)
		return MC_FALSE;
	if (opt_verbose>1)
		log_error("Chdir: '%s'\n", database);
	if (chdir(database) == -1)
	{
		log_error("Error: can't chdir: %s\n", strerror(errno));
		return MC_FALSE;
	}
	if (import_tables(database, tables) == MC_FALSE)
	{
		ret = MC_FALSE;
		if (!opt_force)
			goto end;
	}
	if (opt_function)
	{
		if (import_all_functions(database) == MC_FALSE)
		{
			ret = MC_FALSE;
			if (!opt_force)
				goto end;
		}
	}
	if (opt_procedure)
	{
		if (import_all_procedures(database) == MC_FALSE)
		{
			ret = MC_FALSE;
			if (!opt_force)
				goto end;
		}
	}
	if (opt_trigger)
	{
		if (import_all_triggers(database) == MC_FALSE)
		{
			ret = MC_FALSE;
			if (!opt_force)
				goto end;
		}
	}
	end:
	if (opt_verbose>1)
		log_error("Chdir: '..'\n");
	chdir("..");
	return ret;

}

static int dump_databases(DARRAY *select_databases)
{
	char *database = NULL;
	DARRAY *databases = select_databases;
	size_t i;
	size_t nd=0, ef=0, ed=0;
	int ret = MC_TRUE;
	if (select_databases == NULL)
	{
		databases = dynamic_array_new();
		if (get_dump_databases(databases) == NULL)
		{
			ret = MC_FALSE;
			goto end;
		}
	}

	for (i=0; i<dynamic_array_count(databases); i++)
	{
		database = (char *)dynamic_array_fetch(databases, i);
		if (is_ignore_database(database) == MC_TRUE)
			continue;
		nd++;
		if (opt_verbose)
			log_error("Dump database: `%s`\n", database);
		if (dump_database_form(database) == MC_FALSE)
		{
			ef++;
			if (!opt_force) break;
			continue;
		}
		if (dump_database(database, NULL) == MC_FALSE)
		{
			ed++;
			if (!opt_force) break;
		}
	}
	if (ef || ed)
		ret = MC_FALSE;	
	if (opt_verbose)
		log_error("%ld databases, %ld was failed of form, %ld was failed of table, %ld was failed\n", nd, ef, ed, ef+ed);
	if (select_databases == NULL)
	{
		for (i=0; i<dynamic_array_count(databases); i++)
		{
			database = (char *)dynamic_array_fetch(databases, i);
			free(database);
		}
	}
	end:
	if (select_databases == NULL)
		dynamic_array_destroy(databases);
	return ret;
}


static int import_databases(DARRAY *select_databases)
{
	char *database = NULL;
	DARRAY *databases = select_databases;
	size_t i;
	size_t nd=0, ef=0, ed=0;
	int ret = MC_FALSE;
	if (databases == NULL)
	{
		databases = dynamic_array_new();
		if (get_import_databases(databases, ".") == NULL)
		{
			ret = MC_FALSE;
			goto end;
		}
	}

	for (i=0; i<dynamic_array_count(databases); i++)
	{
		database = (char *)dynamic_array_fetch(databases, i);
		if (is_ignore_database(database) == MC_TRUE)
			continue;
		nd++;
		if (opt_verbose)
			log_error("Import database: `%s`\n", database);
		if (import_database_form(database) == MC_FALSE)
		{
			ef++;
			if (!opt_force) break;
			continue;
		}
		if (import_database(database, NULL) == MC_FALSE)
		{
			ed++;
			if (!opt_force) break;
		}
	}
	if (ef || ed)
		ret = MC_FALSE;
	if (opt_verbose)
		log_error("%ld databases, %ld was failed of form, %ld was failed of table, %ld was failed\n", nd, ef, ed, ef+ed);
	if (select_databases == NULL)
	{
		for (i=0; i<dynamic_array_count(databases); i++)
		{
			database = (char *)dynamic_array_fetch(databases, i);
			free(database);
		}
	}
	end:
	if (select_databases == NULL)
		dynamic_array_destroy(databases);
	return ret;
}

static int dump_mysql(DARRAY *databases, DARRAY *tables)
{
	char *database;
	char *table;
	int ret = MC_TRUE;
	if (opt_verbose)
		log_error("Connect db:\n");
	if (db_connect() == MC_FALSE)
		return MC_FALSE;
	if (opt_single_transaction)
	{
		if (v_mysql_query(mysql, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ") != 0)
		{
			log_error("Error: can't query: %s\n", mysql_error(mysql));
			ret = MC_FALSE;
			goto end;
		}
	}

	if (opt_lock_all)
	{
		if (opt_verbose)
			log_error("Lock all\n");
		if (v_mysql_query(mysql, "FLUSH TABLES WITH READ LOCK") != 0)
		{
			log_error("Error: can't query: %s\n", mysql_error(mysql));
			ret = MC_FALSE;
			goto end;
		}
	}
	if (opt_master_data)
	{
		if (opt_verbose)
			log_error("Dump Master status\n");
		if (dump_master_data() == MC_FALSE)
		{
			log_error("Error: Dump Master status faild\n");
			ret = MC_FALSE;
			goto unlock;
		}
	}
	if (opt_file != NULL)
	{
		database = dynamic_array_fetch(databases, 0);
		if (use_database(database) == MC_TRUE)
		{
			table = dynamic_array_fetch(tables, 0);
			if (opt_lock)
			{
				if (lock_table_write(table) == MC_FALSE)
				{
					ret = MC_FALSE;
					goto unlock;
				}
			}
			if (dump_table_file(table, opt_file) == MC_FALSE)
			{
				log_error("Error: error was happend, dump failed\n");
				ret = MC_FALSE;
			}
			else
			{
				log_error("Import OK\n");
			}
		}
	}
	else
	if (opt_all_databases)
		ret = dump_databases(NULL);
	else
	if (opt_databases)
		ret = dump_databases(databases);
	else
	{
		database = dynamic_array_fetch(databases, 0);
		ret = dump_database(database, dynamic_array_count(tables) ? tables : NULL);
	}
	unlock:
	if (opt_lock_all || opt_lock)
		unlock_tables();
	end:
	if (opt_verbose)
		log_error("Disconnect\n");
	db_disconnect();
	return ret;
}

static int import_mysql(DARRAY *databases, DARRAY *tables)
{
	char *database;
	char *table;
	int ret = MC_TRUE;
	if (opt_verbose)
		log_error("Connect db:\n");
	if (db_connect() == MC_FALSE)
		return MC_FALSE;
	if (opt_lock_all)
	{
		if (opt_verbose)
			log_error("Lock all\n");
		if (v_mysql_query(mysql, "FLUSH TABLES WITH READ LOCK") != 0)
		{
			log_error("Error: can't query: %s\n", mysql_error(mysql));
			ret = MC_FALSE;
			goto end;
		}
	}
	if (opt_file != NULL)
	{
		database = dynamic_array_fetch(databases, 0);
		if (use_database(database) == MC_TRUE)
		{
			if (set_database_character() == MC_FALSE)
				return MC_FALSE;
			table = dynamic_array_fetch(tables, 0);
			if (opt_lock)
			{
				if (lock_table_write(table) == MC_FALSE)
				{
					ret = MC_FALSE;
					goto unlock;
				}
			}
			if (import_table_file(table, opt_file) == MC_FALSE)
			{
				log_error("Error: error was happend, import failed\n");
				ret = MC_FALSE;
			}
			else
				log_error("Import OK\n");
		}
	}
	else
	if (opt_all_databases)
		ret = import_databases(NULL);
	else
	if (opt_databases)
		ret = import_databases(databases);
	else
	{

		database = dynamic_array_fetch(databases, 0);
		ret = import_database(database, dynamic_array_count(tables) ? tables : NULL);
	}
	unlock:
	if (opt_lock_all || opt_lock)
		unlock_tables();
	end:
	if (opt_verbose)
		log_error("Disconnect\n");
	db_disconnect();
	return ret;
}

static void usage_dump()
{
	puts("Author: ChangRong.Zhou");
	//puts("Mail: guxing1841@gmail.com");
	puts("Dumping definition and data mysql database or table");
	puts("");
	printf("%s <--dump|-D> [OPTIONS] <--file> path [OPTIONS] database table\n", progname);
	printf("%s <--dump|-D> [OPTIONS] <--dump-directory|-d> path [OPTIONS] database [tables]\n", progname);
	printf("OR     %s <--dump|-D> [OPTIONS] <--dump-directory|-d> path <--databases|-B> [OPTIONS] DB1 [DB2 DB3...]\n", progname);
	printf("OR     %s <--dump|-d> [OPTIONS] <--dump-directory|-d> path <--all-databases|-A> [OPTIONS]\n", progname);
	puts("");
	puts("The following options may be given as the first argument:");
	puts("  -A, --all-databases Dump all the databases. This will be same as --databases");
	puts("                      with all databases selected.");
	puts("  -B, --databases     To dump several databases. Note the difference in usage;");
	puts("                      In this case no tables are given. All name arguments are");
	puts("                      regarded as databasenames.");
	//puts("      --file          Dump a table data to the file");
	puts("      --function      Dump functions");
	puts("  -C, --compress      Use compression in server/client protocol.");
	puts("      --default-character-set=name");
	puts("                      Set the default character set.");
	//puts("      --defaults-file");
	//puts("                      Only read default options from the given file");
	puts("  -d, --dump-directory=path");
	puts("                      Set dump work directory.");
	puts("      --databases-where");
	puts("                      Dump only where matched databases; QUOTES mandatory!");
	puts("      --databases-pcre");
	puts("                      Dump only matched databases(pcre); QUOTES mandatory!");
	puts("      --tables-pcre");
	puts("                      Dump only matched tables(pcre); QUOTES mandatory!");
	puts("      --tables-form-pcre");
	puts("                      Dump only matched tables of form(pcre); QUOTES mandatory!");
	puts("      --tables-where");
	puts("                      Dump only where matched tables; QUOTES mandatory!");
	puts("      --functions-pcre");
	puts("                      Dump only where matched functions form(pcre); QUOTES mandatory!");
	puts("      --functions-form-pcre");
	puts("                      Dump only where matched functions of form(pcre); QUOTES mandatory!");
	puts("      --procedures-pcre");
	puts("                      Dump only where matched procedures(pcre); QUOTES mandatory!");
	puts("      --procedures-form-pcre");
	puts("                      Dump only where matched procedures of form(pcre); QUOTES mandatory!");
	puts("      --triggers-pcre");
	puts("                      Dump only where matched triggers(pcre); QUOTES mandatory!");
	puts("      --triggers-form-pcre");
	puts("                      Dump only where matched triggers of form(pcre); QUOTES mandatory!");
	puts("  -f, --force         Ignore error");
	puts("  -g, --gzip          Dump to gzip compress file; Add file suffix '.gz'");
	puts("      --pipe          Gzip compression using a separate process through the pipeline");
	puts("  -?, --help          Display this help message and exit.");
	puts("      --ignore        If duplicate unique key was found, keep old row.");
	puts("      --ignore-database");
	puts("                      Dump only not matched databases; MULTIPLE!");
	puts("      --ignore-table");
	puts("                      Dump only not matched tables; MULTIPLE!");
	puts("      --ignore-function");
	puts("                      Dump only not matched functions; MULTIPLE!");
	puts("      --ignore-procedure");
	puts("                      Dump only not matched procedures; MULTIPLE!");
	puts("      --ignore-trigger");
	puts("                      Dump only not matched triggers; MULTIPLE!");
	puts("  -x, --lock-all-tables");
	puts("                      Locks all tables across all databases. This is achieved");
	puts("                      by taking a global read lock for the duration of the");
	puts("                      whole dump. Automatically turns --single-transaction and --lock-tables off.");
	puts("  -l, --lock-tables   Lock all tables of database for read of dump.");
	puts("  -N, --no-data       No row information.");
	puts("      --master-data   Only used by dump, This causes the binary log position and filename to be");
	puts("                      appended to the master_status.sql");
	puts("  -p, --password[=password]");
	puts("                      Password to use when connecting to server. If password is");
	puts("                      not given it's solicited on the tty.");
	puts("  -P, --port=#        Port number to use for connection.");
	puts("      --procedure     Dump procedures");
	puts("      --trigger       Dump triggers");
	puts("      --quick         Don't buffer query to client for dump.");
	puts("  -S, --socket=path   Socket file to use for connection.");
	puts("  -u, --user=name     User for login if not current user.");
	puts("  -v, --verbose       Print info about the various stages.");
	puts("      --where         Dump only selected records; QUOTES mandatory!");
	puts("  --single-transaction");
	puts("                    Creates a consistent snapshot by dumping all tables in a");
	puts("                    single transaction. Works ONLY for tables stored in");
	puts("                    storage engines which support multiversioning (currently");
	puts("                    only InnoDB does); the dump is NOT guaranteed to be");
	puts("                    consistent for other storage engines. While a");
	puts("                    --single-transaction dump is in process, to ensure a");
	puts("                    valid dump file (correct table contents and binary log");
	puts("                    position), no other connection should use the following");
	puts("                    statements: ALTER TABLE, DROP TABLE, RENAME TABLE,");
	puts("                    TRUNCATE TABLE, as consistent snapshot is not isolated");
	puts("                    from them. Option automatically turns off --lock-tables.");
	puts("");
}

static void usage_import()
{
	puts("Author: ChangRong.Zhou");
	//puts("Mail: guxing1841@gmail.com");
	puts("Importing definition and data mysql database or table");
	puts("");
	printf("%s <--import|-I> [OPTIONS] <--file> path [OPTIONS] database table\n", progname);
	printf("%s <--import|-I> [OPTIONS] <--import-directory|-d> path [OPTIONS] database [tables]\n", progname);
	printf("OR     %s <--import|-I> [OPTIONS] <--import-directory|-d> path <--databases|-B> [OPTIONS] DB1 [DB2 DB3...]\n", progname);
	printf("OR     %s <--import|-I> [OPTIONS] <--import-directory|-d> path <--all-databases|-A> [OPTIONS]\n", progname);
	puts("");
	puts("The following options may be given as the first argument:");
	puts("  -A, --all-databases Import all the databases. This will be same as --databases");
	puts("                      with all databases selected.");
	puts("  -B, --databases     Import several databases. Note the difference in usage;");
	puts("                      In this case no tables are given. All name arguments are");
	puts("                      regarded as databasenames.");
	puts("      --file          Import a table data from the file");
	puts("      --function      Import functions");
	puts("  -C, --compress      Use compression in server/client protocol.");
	puts("      --default-character-set=name");
	puts("                      Set the default character set.");
	//puts("      --defaults-file");
	//puts("                      Only read default options from the given file");
	puts("      --disable-keys  Disable keys before import table data, (fast!!!).");
	puts("  -d, --import-directory=path");
	puts("                      Set import work directory.");
	puts("      --databases-where");
	puts("                      Dump only where matched databases; QUOTES mandatory!");
	puts("      --databases-pcre");
	puts("                      Import only matched databases(pcre); QUOTES mandatory!");
	puts("      --tables-pcre");
	puts("                      Import only matched tables(pcre); QUOTES mandatory!");
	puts("      --tables-form-pcre");
	puts("                      Import only matched tables of form(pcre); QUOTES mandatory!");
	puts("      --tables-where");
	puts("                      Import only where matched tables; QUOTES mandatory!");
	puts("      --functions-pcre");
	puts("                      Import only where matched functions form(pcre); QUOTES mandatory!");
	puts("      --functions-form-pcre");
	puts("                      Import only where matched functions of form(pcre); QUOTES mandatory!");
	puts("      --procedures-pcre");
	puts("                      Import only where matched procedures(pcre); QUOTES mandatory!");
	puts("      --procedures-form-pcre");
	puts("                      Import only where matched procedures of form(pcre); QUOTES mandatory!");
	puts("      --triggers-pcre");
	puts("                      Import only where matched triggers(pcre); QUOTES mandatory!");
	puts("      --triggers-form-pcre");
	puts("                      Import only where matched triggers of form(pcre); QUOTES mandatory!");
	puts("      --drop-table    Drop table before import");
	puts("  -f, --force         Ignore error");
	puts("  -g, --gzip[=level]  Import from gzip compress file; Add file suffix '.gz'");
	puts("      --ignore        If duplicate unique key was found, keep old row.");
	puts("      --ignore-database");
	puts("                      Import only not matched databases; MULTIPLE!");
	puts("      --ignore-table");
	puts("                      Import only not matched tables; MULTIPLE!");
	puts("      --ignore-function");
	puts("                      Import only not matched functions; MULTIPLE!");
	puts("      --ignore-procedure");
	puts("                      Import only not matched procedures; MULTIPLE!");
	puts("      --ignore-trigger");
	puts("                      Import only not matched triggers; MULTIPLE!");
	puts("  -l, --lock-tables   Lock table for write of import.");
	puts("  -N, --no-data       No row information.");
	puts("  -p, --password[=password]");
	puts("                      Password to use when connecting to server. If password is");
	puts("                      not given it's solicited on the tty.");
	puts("  -P, --port=#        Port number to use for connection.");
	puts("      --procedure     import procedures");
	puts("      --trigger       import triggers");
	puts("      --replace       If duplicate unique key was found, replace old row.");
	puts("  -S, --socket=path   Socket file to use for connection.");
	puts("  -u, --user=name     User for login if not current user.");
	puts("  -v, --verbose       Print info about the various stages.");
	puts("      --view          Import only view, not other tables");
	puts("  -T, --tempdir=path  Template directory.");
	puts("");
}

static void usage()
{
	puts("Author: ChangRong.Zhou");
	//puts("Mail: guxing1841@gmail.com");
	puts("Dumping or Importing definition and data mysql database or table");
	puts("");
	puts("Usage:");
	printf("%s [--help|-?|--version|-V]\n", progname);
	puts("The following options may be given as the first argument:");
	puts("  -D, --dump          Dump.");
	puts("  -I, --import        Import.");
	puts("  -?, --help          Display this help message and exit.");
	puts("  -V, --version       Print version.");
	puts("");
	/*
	puts("Usage for dump:");
	usage_dump();
	puts("");
	puts("Usage for import:");
	usage_import();
	puts("");
	*/
}

//static const char *load_default_groups[]= { "mysqlcsv", "mysqldump", "client",0 };

#define LONG_OPTIONS_EQ(option) \
	(strcmp(long_options[option_index].name, option) == 0)

static int getopt_dump(int argc, char **argv, DARRAY *databases, DARRAY *tables)
{
	int c;
	DHASH *args = NULL;
	/* Default dump ignore */
	if (ignore_tables == NULL)
		ignore_tables = dynamic_hash_new();
	dynamic_hash_store(ignore_tables, "mysql.general_log", strlen("mysql.general_log"), NULL);
	dynamic_hash_store(ignore_tables, "mysql.slow_log", strlen("mysql.slow_log"), NULL);
	if (ignore_databases == NULL)
		ignore_databases = dynamic_hash_new();
	dynamic_hash_store(ignore_databases, "information_schema", strlen("information_schema"), NULL);
	dynamic_hash_store(ignore_databases, "performance_schema", strlen("performance_schema"), NULL);
	if (ignore_data_types == NULL)
		ignore_data_types = dynamic_hash_new();
	dynamic_hash_store(ignore_data_types, "MRG_MyISAM", strlen("MRG_MyISAM"), NULL);
	dynamic_hash_store(ignore_data_types, "MRG_ISAM", strlen("MRG_ISAM"), NULL);
	dynamic_hash_store(ignore_data_types, "FEDERATED", strlen("FEDERATED"), NULL);
	dynamic_hash_store(ignore_data_types, "VIEW", strlen("VIEW"), NULL);
	if (ignore_data_tables == NULL)
		ignore_data_tables = dynamic_hash_new();
	if (ignore_data_databases == NULL)
		ignore_data_databases = dynamic_hash_new();
	/* End default dump ignore */

	while (1)
	{
		int option_index = 0;
		static struct option long_options[] =
		{
			{"all-databases", 0, 0, 'A'},
			{"debug", 0, 0, 0},
			{"default-character-set", 1, 0, 0},
			//{"defaults-file", 1, 0, 0},
			{"file", 1, 0, 0},
			{"compress", 0, 0, 'C'},
			{"databases", 0, 0, 'B'},
			{"dump-directory", 1, 0, 'd'},
			{"databases-where", 1, 0, 0},
			{"function", 0, 0, 0},
			{"tables-where", 1, 0, 0},
			{"where", 1, 0, 6},
			{"databases-pcre", 1, 0, 0},
			{"tables-pcre", 1, 0, 0},
			{"functions-pcre", 1, 0, 0},
			{"procedures-pcre", 1, 0, 0},
			{"triggers-pcre", 1, 0, 0},
			{"force", 0, 0, 'f'},
			{"gzip", 2, 0, 'g'},
			{"host", 1, 0, 'h'},
			{"help", 0, 0, '?'},
			{"pipe", 0, 0, 0},
			{"ignore-database", 1, 0, 0},
			{"ignore-table", 1, 0, 0},
			{"ignore-function", 1, 0, 0},
			{"ignore-procedure", 1, 0, 0},
			{"ignore-trigger", 1, 0, 0},
			{"lock-all-tables", 0, 0, 'x'},
			{"lock-tables", 0, 0, 'l'},
			{"no-data", 0, 0, 'N'},
			{"master-data", 0, 0, 0},
			{"procedure", 0, 0, 0},
			{"trigger", 0, 0, 0},
			{"port", 1, 0, 'P'},
			{"password", 2, 0, 'p'},
			{"quick", 0, 0, 0},
			{"socket", 1, 0, 'S'},
			{"user", 1, 0, 'u'},
			{"verbose", 0, 0, 'v'},
			{"single-transaction", 0, 0, 0},
			{0, 0, 0, 0}
		};
		c = getopt_long(argc, argv, "ACBd:fg::h:?lNP:p::S:u:vx",
			long_options, &option_index);
		if (c == -1)
			break;
		switch (c)
		{
			case 0:
				if (LONG_OPTIONS_EQ("debug"))
					opt_debug = 1;
				else
				if (LONG_OPTIONS_EQ("pipe"))
					opt_pipe = 1;
				else
				if (LONG_OPTIONS_EQ("default-character-set"))
					opt_default_character_set = optarg;
				else
				//if (LONG_OPTIONS_EQ("defaults-file"))
				//	opt_defaults_file = optarg;
				//else
				if (LONG_OPTIONS_EQ("ignore-database"))
				{
					if (ignore_databases == NULL)
						ignore_databases = dynamic_hash_new();
					dynamic_hash_store(ignore_databases, optarg, strlen(optarg), NULL);
				}
				else
				if (LONG_OPTIONS_EQ("ignore-table"))
				{
					if (ignore_tables == NULL)
						ignore_tables = dynamic_hash_new();
					dynamic_hash_store(ignore_tables, optarg, strlen(optarg), NULL);
				}
				else
				if (LONG_OPTIONS_EQ("master-data"))
					opt_master_data = 1;
				else
				if (LONG_OPTIONS_EQ("databases-where"))
					databases_where = optarg;
				else
				if (LONG_OPTIONS_EQ("quick"))
					opt_quick = 1;
				else
				if (LONG_OPTIONS_EQ("tables-where"))
					tables_where = optarg;
				else
				if (LONG_OPTIONS_EQ("where"))
					where = optarg;
				else
				if (LONG_OPTIONS_EQ("databases-pcre"))
					databases_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("tables-pcre"))
					tables_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("function"))
					opt_function = 1;
				else
				if (LONG_OPTIONS_EQ("procedure"))
					opt_procedure = 1;
				else
				if (LONG_OPTIONS_EQ("trigger"))
					opt_trigger = 1;
				else
				if (LONG_OPTIONS_EQ("functions-pcre"))
					functions_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("procedures-pcre"))
					procedures_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("triggers-pcre"))
					triggers_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("file"))
					opt_file = optarg;
				else
				if (LONG_OPTIONS_EQ("single-transaction"))
					opt_single_transaction = 1;
				else
				{
					log_error("Error: Unkown long option '--%s'\n", long_options[option_index].name); 
					usage();
					return MC_FALSE;
				}
				break;
			case '?':
				usage_dump();
				exit(0);
			case 'A':
				opt_all_databases = 1;
				break;
			case 'B':
				opt_databases = 1;
				break;
			case 'C':
				opt_compress = 1;
				break;
			case 'd':
				opt_directory = optarg;
				break;
			case 'f':
				opt_force = 1;
				break;
			case 'g':
				opt_gzip = 1;
				if (optarg != NULL)
					opt_gzip = atoi(optarg);
				break;
			case 'h':
				opt_host = optarg;
				break;
			case 'l':
				opt_lock = 1;
				break;
			case 'N':
				opt_no_data = 1;
				break;
			case 'P':
				opt_port = atoi(optarg);
				break;
			case 'p':
				opt_password = optarg;
				if (opt_password == NULL)
				{
					opt_getpass = 1;
				}
				break;
			case 'S':
				opt_unix_socket = optarg;
				break;
			case 'u':
				opt_user = optarg;
				break;
			case 'v':
				opt_verbose++;
				break;
			case 'x':
				opt_lock_all = 1;
				break;
			default:
				log_error("Error: with option '%c'\n", c);
				usage_dump();
				return MC_FALSE;
		}
	}
	if (opt_directory == NULL)
	{
		log_error("Dump directory is not set\n");
		return MC_FALSE;
	}

	if (opt_gzip < 0 || opt_gzip > 9)
	{
		log_error("Error: invalid gzip compress level %d\n", opt_gzip);
		return MC_FALSE;
	}

	if (optind < argc)
	{
		if (opt_file != NULL)
		{
			if (argc - optind != 2)
			{
				log_error("Error: args error\n");
				usage_dump(argv[0]);
				return MC_FALSE;
			}
			dynamic_array_push(databases, string_copy(argv[optind++]));
			dynamic_array_push(tables, string_copy(argv[optind++]));
		}
		else
		{
			if (opt_all_databases)
			{
				log_error("Error: args error\n");
				usage_dump();
				return MC_FALSE;
			}
			args = dynamic_hash_new();
			if (opt_databases)
			{
				while(optind < argc)
				{
					if (dynamic_hash_haskey(args, argv[optind], strlen(argv[optind])) == MC_TRUE)
					{
						optind++;
						continue;
					}
					dynamic_array_push(databases, string_copy(argv[optind]));
					dynamic_hash_store(args, argv[optind], strlen(argv[optind]), NULL);
					optind++;
				} 
			}
			else
			{
				dynamic_array_push(databases, string_copy(argv[optind++]));
				while(optind < argc)
				{
					if (dynamic_hash_haskey(args, argv[optind], strlen(argv[optind])) == MC_TRUE)
					{
						optind++;
						continue;
					}
					dynamic_array_push(tables, string_copy(argv[optind]));
					dynamic_hash_store(args, argv[optind], strlen(argv[optind]), NULL);
					optind++;

				} 
			}
			if (args != NULL)
			dynamic_hash_destroy(args);
		}
	}
	else
	if (!opt_all_databases)
	{
		log_error("Error: args error\n");
		usage_dump();
		return MC_FALSE;
	}
	return MC_TRUE;
}

static int getopt_import (int argc, char **argv, DARRAY *databases, DARRAY *tables)
{
	int c;
	DHASH *args = NULL;
	/* Default import ignore */
	if (ignore_tables == NULL)
		ignore_tables = dynamic_hash_new();
	dynamic_hash_store(ignore_tables, "mysql.general_log", strlen("mysql.general_log"), NULL);
	dynamic_hash_store(ignore_tables, "mysql.slow_log", strlen("mysql.slow_log"), NULL);

	if (ignore_databases == NULL)
		ignore_databases = dynamic_hash_new();
	dynamic_hash_store(ignore_databases, "information_schema", strlen("information_schema"), NULL);
	dynamic_hash_store(ignore_databases, "performance_schema", strlen("performance_schema"), NULL);
	if (ignore_data_types == NULL)
		ignore_data_types = dynamic_hash_new();
	dynamic_hash_store(ignore_data_types, "MRG_MyISAM", strlen("MRG_MyISAM"), NULL);
	dynamic_hash_store(ignore_data_types, "MRG_ISAM", strlen("MRG_ISAM"), NULL);
	dynamic_hash_store(ignore_data_types, "FEDERATED", strlen("FEDERATED"), NULL);
	dynamic_hash_store(ignore_data_types, "VIEW", strlen("VIEW"), NULL);
	if (ignore_data_tables == NULL)
		ignore_data_tables = dynamic_hash_new();
	if (ignore_data_databases == NULL)
		ignore_data_databases = dynamic_hash_new();
	/* End default import ignore */
	while (1)
	{
		int option_index = 0;
		static struct option long_options[] =
		{
			{"all-databases", 0, 0, 'A'},
			{"debug", 0, 0, 0},
			{"default-character-set", 1, 0, 0},
			{"defaults-file", 1, 0, 0},
			{"file", 1, 0, 0},
			{"function", 0, 0, 0},
			{"compress", 0, 0, 'C'},
			{"databases", 0, 0, 'B'},
			{"disable-keys", 0, 0, 0},
			{"import-directory", 1, 0, 'd'},
			{"databases-pcre", 1, 0, 0},
			{"tables-pcre", 1, 0, 0},
			{"tables-form-pcre", 1, 0, 0},
			{"drop-table", 0, 0, 0},
			{"force", 0, 0, 'f'},
			{"gzip", 0, 0, 'g'},
			{"host", 1, 0, 'h'},
		  	{"help", 0, 0, '?'},
			{"ignore", 0, 0, 0},
			{"ignore-database", 1, 0, 0},
			{"ignore-table", 1, 0, 0},
			{"ignore-function", 1, 0, 0},
			{"ignore-procedure", 1, 0, 0},			
			{"ignore-trigger", 1, 0, 0},			
			{"lock-tables", 0, 0, 'l'},
			{"no-data", 0, 0, 'N'},
			{"port", 1, 0, 'P'},
			{"procedure", 0, 0, 0},
			{"trigger", 0, 0, 0},
			{"password", 2, 0, 'p'},
			{"replace", 0, 0, 0},
			{"socket", 1, 0, 'S'},
			{"user", 1, 0, 'u'},
			{"verbose", 0, 0, 'v'},
			{"view", 0, 0, 0},
			{"tmpdir", 1, 0, 'T'},
			{0, 0, 0, 0}
		};
		c = getopt_long(argc, argv, "ACBd:fgh:?lNP:p::S:u:vT:",
			long_options, &option_index);
		if (c == -1)
			break;
		switch (c)
		{
			case 0:
				if (LONG_OPTIONS_EQ("debug"))
					opt_debug = 1;
				else
				if (LONG_OPTIONS_EQ("default-character-set"))
					opt_default_character_set = optarg;
				else
				//if (LONG_OPTIONS_EQ("defaults-file")) 
				//	opt_defaults_file = optarg;
				//else
				if (LONG_OPTIONS_EQ("ignore-database"))
				{
					if (ignore_databases == NULL)
						ignore_databases = dynamic_hash_new();
					dynamic_hash_store(ignore_databases, optarg, strlen(optarg), NULL);
				}
				else
				if (LONG_OPTIONS_EQ("ignore-table"))
				{
					if (ignore_tables == NULL)
						ignore_tables = dynamic_hash_new();
					dynamic_hash_store(ignore_tables, optarg, strlen(optarg), NULL);
				}
				else
				if (LONG_OPTIONS_EQ("ignore-function"))
				{
					if (ignore_functions == NULL)
						ignore_functions = dynamic_hash_new();
					dynamic_hash_store(ignore_functions, optarg, strlen(optarg), NULL);
				}
				else
				if (LONG_OPTIONS_EQ("ignore-procedure"))
				{
					if (ignore_procedures == NULL)
						ignore_procedures = dynamic_hash_new();
					dynamic_hash_store(ignore_procedures, optarg, strlen(optarg), NULL);
				}
				else
				if (LONG_OPTIONS_EQ("ignore-trigger"))
				{
					if (ignore_triggers == NULL)
						ignore_triggers = dynamic_hash_new();
					dynamic_hash_store(ignore_triggers, optarg, strlen(optarg), NULL);
				}
				else
				if (LONG_OPTIONS_EQ("master-data"))
					opt_master_data = 1;
				else
				if (LONG_OPTIONS_EQ("databases-pcre"))
					databases_pcre = optarg;
                else
				if (LONG_OPTIONS_EQ("disable-keys"))
					opt_disable_keys = 1;
				else
				if (LONG_OPTIONS_EQ("tables-pcre"))
					tables_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("tables-form-pcre"))
					tables_form_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("file"))
					opt_file = optarg;
				else
				if (LONG_OPTIONS_EQ("replace"))
					opt_replace = 1;
				else
				if (LONG_OPTIONS_EQ("ignore"))
					opt_ignore = 1;
				else
				if (LONG_OPTIONS_EQ("drop-table"))
					opt_drop_table = 1;
				else
				if (LONG_OPTIONS_EQ("function"))
					opt_function = 1;
				else
				if (LONG_OPTIONS_EQ("procedure"))
					opt_procedure = 1;
				else
				if (LONG_OPTIONS_EQ("trigger"))
					opt_trigger = 1;
				else
				if (LONG_OPTIONS_EQ("functions-pcre"))
					functions_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("procedures-pcre"))
					procedures_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("triggers-pcre"))
					triggers_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("functions-form-pcre"))
					functions_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("procedures-form-pcre"))
					procedures_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("triggers-form-pcre"))
					triggers_pcre = optarg;
				else
				if (LONG_OPTIONS_EQ("view"))
					opt_view = 1;
				else
				{
					log_error("Error: Unkown long option '--%s'\n", long_options[option_index].name); 
					usage();
					return MC_FALSE;
				}
				break;
			case '?':
				usage_import(argv[0]);
				exit(0);
			case 'A':
				opt_all_databases = 1;
				break;
			case 'B':
				opt_databases = 1;
				break;
			case 'C':
				opt_compress = 1;
				break;
			case 'd':
				opt_directory = optarg;
				break;
			case 'f':
				opt_force = 1;
				break;
			case 'g':
				opt_gzip = 1;
				break;
			case 'h':
				opt_host = optarg;
				break;
			case 'l':
				opt_lock = 1;
				break;
			case 'N':
				opt_no_data = 1;
				break;
			case 'P':
				opt_port = atoi(optarg);
				break;
			case 'p':
				opt_password = optarg;
				if (opt_password == NULL)
				{
					opt_getpass = 1;
				}
				break;
			case 'S':
				opt_unix_socket = optarg;
				break;
			case 'u':
				opt_user = optarg;
				break;
			case 'v':
				opt_verbose++;
				break;
			case 'T':
				opt_tmpdir = optarg;
				break;
			default:
				log_error("Error: with option '%c'\n", c);
				usage_import(argv[0]);
				return MC_FALSE;
		}
	}
	if (opt_ignore && opt_replace)
	{
		log_error("Error: Can't both use '--ignore' and '--replace' option\n");
		return MC_FALSE;
	}
	if (opt_file == NULL)
	{
		
		if (opt_directory == NULL)
		{
			log_error("Import directory is not set\n");
			return MC_FALSE;
		}
	}

	if (optind < argc)
	{
		if (opt_file != NULL)
		{
			if (argc - optind != 2)
			{
				log_error("Error: args error\n");
				usage_import(argv[0]);
				return MC_FALSE;
			}
			dynamic_array_push(databases, string_copy(argv[optind++]));
			dynamic_array_push(tables, string_copy(argv[optind++]));
		}
		else
		{
			if (opt_all_databases)
			{
				log_error("Error: args error\n");
				usage_import(argv[0]);
				return MC_FALSE;
			}
			args = dynamic_hash_new();
			if (opt_databases)
			{
				while(optind < argc)
				{
					if (dynamic_hash_haskey(args, argv[optind], strlen(argv[optind])) == MC_TRUE)
					{
						optind++;
						continue;
					}
					dynamic_array_push(databases, string_copy(argv[optind]));
					dynamic_hash_store(args, argv[optind], strlen(argv[optind]), NULL);
					optind++;
				} 
			}
			else
			{
				dynamic_array_push(databases, string_copy(argv[optind++]));
				while(optind < argc)
				{
					if (dynamic_hash_haskey(args, argv[optind], strlen(argv[optind])) == MC_TRUE)
					{
						optind++;
						continue;
					}
					dynamic_array_push(tables, string_copy(argv[optind]));
					dynamic_hash_store(args, argv[optind], strlen(argv[optind]), NULL);
					optind++;
	
				} 
			}
			if (args != NULL)
			dynamic_hash_destroy(args);
		}
	}
	else
	if (!opt_all_databases)
	{
		log_error("Error: args error\n");
		usage_import(argv[0]);
		return MC_FALSE;
	}
	return MC_TRUE;
}


int main(int argc, char **argv)
{
	int result = 0;
	int ret = 0;
	size_t i;
	DARRAY *databases = NULL;
	DARRAY *tables = NULL;
	double s, u;
	progname = argv[0];
	if (getcwd(workdir, PATH_MAX) == NULL)
	{
		log_error("Error: getcwd() failed: %s\n", strerror(errno));
		exit(1);
	}
	opt_tmpdir = workdir;
	if (argc == 1)
	{
		usage();
		exit(1);
	}
	else
	if (strcmp(argv[1], "--dump") == 0 || strcmp(argv[1], "-D") == 0)
	{
		opt_dump = 1;
	}
	else
	if (strcmp(argv[1], "--import") == 0 || strcmp(argv[1], "-I") == 0)
	{
		opt_import = 1;
	}
	else
	if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
	{
		usage();
		exit(0);
	}
	else
	if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
	{
		printf("Version %s\n", MC_VERSION);
		exit(0);
	}
	else
	{
		log_error("Error: unknow argument '%s'\n", argv[1]);
		usage();
		exit(1);
	}

	databases = dynamic_array_new();
	tables = dynamic_array_new();
	sql = dynamic_string_new(128);
	dstring = dynamic_string_new(128);
	if (opt_dump)
	{
		if (getopt_dump(argc-1, argv+1, databases, tables) == MC_FALSE)
		{
			result = 1;
			goto end;
		}
	}
	else
	if (opt_import)
	{
		if (getopt_import(argc-1, argv+1, databases, tables) == MC_FALSE)
		{
			result = 1;
			goto end;
		}
	}
	if (opt_file == NULL && chdir(opt_directory) == -1)
	{
		log_error("Error: can't chdir: %s\n", strerror(errno));
		result = 1;
		goto end;
	}


	if ((opt_all_databases && dynamic_array_count(databases)) || !(opt_all_databases || dynamic_array_count(databases)))
	{
		usage();
		result = 1;
		goto end;
	}
	if (opt_master_data)
	{
		opt_lock_all = 1;
	}
	if (opt_lock_all)
	{
		opt_single_transaction = 0;
		opt_lock = 0;
	}
	if (opt_single_transaction)
		opt_lock = 0;
	if (databases_pcre != NULL)
	{
		databases_re = mc_pcre_complie(databases_pcre);
		if (databases_re == NULL)
		{
			result = 1;
			goto end;
		}
	}
	if (tables_pcre != NULL)
	{
		tables_re = mc_pcre_complie(tables_pcre);
		if (tables_re == NULL)
		{
			result = 1;
			goto end;
		}
	}

	if (opt_view)
		tables_form_pcre = tables_form_view_pcre;
	else
	if (tables_form_pcre == NULL)
		tables_form_pcre = tables_form_default_pcre;
	tables_form_re = mc_pcre_complie(tables_form_pcre);
	if (tables_form_re == NULL)
	{
		result = 1;
		goto end;
	}
	tables_form_replace_re = mc_pcre_complie(tables_form_replace_pcre);
	if (tables_form_replace_re == NULL)
	{
		result = 1;
		goto end;
	}
	if (opt_function)
	{
		if (functions_pcre != NULL)
		{
			functions_re = mc_pcre_complie(functions_pcre);
			if (functions_re == NULL)
			{
				result = 1;
				goto end;
			}
		}
		if (functions_form_pcre != NULL)
		{
			functions_form_re = mc_pcre_complie(functions_form_pcre);
			if (functions_form_re == NULL)
			{
				result = 1;
				goto end;
			}
		}
	}
	if (opt_procedure)
	{
		if (procedures_pcre != NULL)
		{
			procedures_re = mc_pcre_complie(procedures_pcre);
			if (procedures_re == NULL)
			{
				result = 1;
				goto end;
			}
		}
		if (procedures_form_pcre != NULL)
		{
			procedures_form_re = mc_pcre_complie(procedures_form_pcre);
			if (procedures_form_re == NULL)
			{
				result = 1;
				goto end;
			}
		}
	}

	if (opt_trigger)
	{
		if (triggers_pcre != NULL)
		{
			triggers_re = mc_pcre_complie(triggers_pcre);
			if (triggers_re == NULL)
			{
				result = 1;
				goto end;
			}
		}
		if (triggers_form_pcre != NULL)
		{
			triggers_form_re = mc_pcre_complie(triggers_form_pcre);
			if (triggers_form_re == NULL)
			{
				result = 1;
				goto end;
			}
		}
	}
	if (opt_getpass && opt_password == NULL)
		opt_password = getpass("Password: ");
	s = my_time();
	if (opt_dump)
		ret = dump_mysql(databases, tables);
	else
	if (opt_import)
	{
		ret = import_mysql(databases, tables);
		if (fifo_file != NULL)
		{
			unlink(fifo_file->data);
			dynamic_string_destroy(fifo_file);
			fifo_file = NULL;
		}
	}
	u = my_time()-s;
	if (opt_verbose)
		log_error("Total use time: %.3f sec\n", u);
	result = ret ? result : 1;
	result = opt_force ? 0 : result;
	end:
	for (i=0; tables != NULL && i<dynamic_array_count(tables);i++)
		free(dynamic_array_fetch(tables, i));
	for (i=0; databases != NULL &&  i<dynamic_array_count(databases); i++)
		free(dynamic_array_fetch(databases, i));
	if (tables != NULL)
		dynamic_array_destroy(tables);
	if (databases != NULL)
		dynamic_array_destroy(databases);
	if (sql != NULL)
		dynamic_string_destroy(sql);
	if (dstring != NULL)
		dynamic_string_destroy(dstring);
	if (ignore_tables != NULL)
		dynamic_hash_destroy(ignore_tables);
	if (ignore_databases != NULL)
		dynamic_hash_destroy(ignore_databases);
	if (ignore_functions != NULL)
		dynamic_hash_destroy(ignore_functions);
	if (ignore_procedures != NULL)
		dynamic_hash_destroy(ignore_procedures);
	if (ignore_triggers != NULL)
		dynamic_hash_destroy(ignore_triggers);
	if (ignore_data_types != NULL)
		dynamic_hash_destroy(ignore_data_types);
	if (ignore_data_tables != NULL)
		dynamic_hash_destroy(ignore_data_tables);
	if (ignore_data_databases != NULL)
		dynamic_hash_destroy(ignore_data_databases);
	return result;
}
