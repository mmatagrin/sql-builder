package aurora

import (
	"github.com/mmatagrin/ctxerror"
	)

type AuroraUpdateStruct struct {
	sqlStr string
	where  []string
}

func AuroraUpdate(table string, expressions []string) *AuroraUpdateStruct {
	var sqlStr string

	sqlStr = "UPDATE " + table + " SET "

	for _, expression := range expressions {
		sqlStr += expression + ","
	}
	sqlStr = sqlStr[:len(sqlStr)-1]

	return &AuroraUpdateStruct{sqlStr: sqlStr}
}

func (mu *AuroraUpdateStruct) Where(condition string) *AuroraUpdateStruct {
	mu.where = append(mu.where, condition)
	return mu
}

func (mu *AuroraUpdateStruct) ExecuteUpdate(connexion AuroraConnexion, values map[string]interface{}, transactionId *string) (int64, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"connexion": connexion,
		"values": values,
	})

	for index, condition := range mu.where {
		if index == 0 {
			mu.sqlStr += " WHERE " + condition
		} else {
			mu.sqlStr += " AND " + condition
		}
	}

	res, err := PerformAuroraQuery(mu.sqlStr, values, connexion, transactionId)
	if err != nil {
		return 0, context.Wrap(err, "unable to perform update query")
	}

	if res.NumberOfRecordsUpdated != nil {
		return *res.NumberOfRecordsUpdated, nil
	}

	return  0, nil
}
