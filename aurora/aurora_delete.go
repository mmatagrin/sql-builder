package aurora

import (
	"fmt"
	"github.com/mmatagrin/ctxerror"
)

type AuroraDeleteStruct struct {
	tableName string
	where  []string
	orWhere []string
}

func AuroraDelete(tableName string) *AuroraDeleteStruct {
	return &AuroraDeleteStruct{tableName: tableName}
}

func (ads *AuroraDeleteStruct) Where(condition string) *AuroraDeleteStruct {
	ads.where = append(ads.where, condition)
	return ads
}

func (ads *AuroraDeleteStruct) OrWhere(condition string) *AuroraDeleteStruct {
	ads.orWhere = append(ads.where, condition)
	return ads
}



func (ads *AuroraDeleteStruct) ExecuteDelete(connexion AuroraConnexion, values map[string]interface{}, transactionId *string) (int64, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"connexion": connexion,
		"values": values,
	})

	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE 1 ", ads.tableName)

	for _, condition := range ads.where {

		sqlStr += " AND (" + condition + ")"

	}

	for _, condition  := range ads.orWhere {
		sqlStr += " OR (" + condition + ")"
	}

	res, err := PerformAuroraQuery(sqlStr, values, connexion, transactionId)
	if err != nil {
		return 0, context.Wrap(err, "unable to perform delete query")
	}

	if res.NumberOfRecordsUpdated != nil {
		return *res.NumberOfRecordsUpdated, nil
	}

	return  0, nil
}
