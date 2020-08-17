package aurora

import (
		"strings"
	"fmt"
	"github.com/mmatagrin/ctxerror"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
	)

//TODO replace inserts with the main query builder

const (
	INSERT_NOT_IGNORE = iota
	INSERT_IGNORE
	REPLACE
)

func AuroraInsert(table string, columns []string, values [][]interface{}, connexion AuroraConnexion, transactionId *string)(*rdsdataservice.ExecuteStatementOutput, error){
	return insert(table, columns, values, connexion, INSERT_NOT_IGNORE, transactionId)
}

func AuroraInsertIgnore(table string, columns []string, values [][]interface{}, connexion AuroraConnexion, transactionId *string)(*rdsdataservice.ExecuteStatementOutput, error){
	return insert(table, columns, values, connexion, INSERT_IGNORE, transactionId)
}

func AuroraReplace(table string, columns []string, values [][]interface{}, connexion AuroraConnexion, transactionId *string)(*rdsdataservice.ExecuteStatementOutput, error) {
	return insert(table, columns, values, connexion, REPLACE, transactionId)
}

func insert(table string, columns []string, values [][]interface{}, connexion AuroraConnexion, mode int, transactionId *string)(*rdsdataservice.ExecuteStatementOutput, error){
	context := ctxerror.SetContext(map[string]interface{}{
		"table": table,
		"columns": columns,
		"values": values,
		"connexion": connexion,
		"mode": mode,
		"transactionId": fmt.Sprintf("%s", transactionId),
	})

	var sqlStr string

	var parameters = make(map[string]interface{})

	switch mode {
	case INSERT_NOT_IGNORE:
		sqlStr = "INSERT INTO " + table + " "
	case INSERT_IGNORE:
		sqlStr = "INSERT IGNORE INTO " + table + " "
	case REPLACE:
		sqlStr = "REPLACE INTO " + table + " "
	}

	sqlStr += "(" + strings.Join(columns, ",") + ") VALUES "

	for i, val := range values {
		sqlStr += "("

		for j, fieldVal := range val{
			sqlStr += fmt.Sprintf(":%s%d,", columns[j], i)
			parameters[fmt.Sprintf("%s%d", columns[j], i)] = fieldVal
		}

		sqlStr = strings.TrimSuffix(sqlStr, ",") + "),"
	}

	//remove the useless last ","
	sqlStr = strings.TrimSuffix(sqlStr, ",")

	res, err := PerformAuroraQuery(sqlStr, parameters, connexion, transactionId)
	if err != nil {
		context.AddContext("sql_query", sqlStr)
		context.AddContext("sql_query_parameters", parameters)
		return nil, context.Wrap(err, "unable to perform insert query")
	}

	if mode == INSERT_NOT_IGNORE || mode == REPLACE {
		if len(res.GeneratedFields) == 0 {
			return nil, context.New("inserted line, but no result was returned")
		}

		returnedField := res.GeneratedFields[0]

		if returnedField == nil {
			return nil, context.New("inserted line but empty result returned")
		}
	}

	return  res, nil
}
