package aurora

import (
	"github.com/mmatagrin/ctxerror"
	structs "github.com/mmatagrin/sql-builder/structs"
	"strconv"
	"strings"
)

type QueryType string

const (
	SELECT QueryType  =  "select"
	UPDATE = "update"
	DELETE = "delete"
)
type AuroraQuery struct {
	QueryType          QueryType
	AuroraQueryBuilder AuroraQueryBuilder
	SqlStr             string
	parameters         map[string]interface{}
}

func (aq *AuroraQuery) GetSql() string {
	if len(aq.SqlStr) == 0 {
		aq.SqlStr = aq.PrepareSql(aq.AuroraQueryBuilder.query)
	}

	return aq.SqlStr
}

func (aq *AuroraQuery) GetResults(connexion AuroraConnexion, transactionId *string) ([]QueryResult, error) {
	context := ctxerror.SetContext(map[string]interface{}{
		"connexion": connexion,
		"query": aq.GetSql(),
	})

	res, err := PerformAuroraQuery(aq.GetSql(), aq.parameters, connexion, transactionId)

	if err != nil{
		return nil, context.Wrap(err, "unable to perform query")
	}

	if res == nil{
		return nil, context.New("query result is <nil>")
	}

	if res.Records != nil {
		fields := []string{}
		for _, field := range aq.AuroraQueryBuilder.query.Select {
			splitField := strings.Split(strings.ToLower(field), " as ")
			if len(splitField) > 1 {
				fields = append(fields, splitField[1])
				continue
			}

			fields = append(fields, field)
		}

		return ParseResults(fields, res.Records)
	}

	return []QueryResult{}, nil
}

func (aq *AuroraQuery) Execute (connexion AuroraConnexion, transactionId *string) (int64, error){

	context := ctxerror.SetContext(map[string]interface{}{
		"connexion": connexion,
		"query": aq.GetSql(),
	})

	res, err := PerformAuroraQuery(aq.GetSql(), aq.parameters, connexion, transactionId)
	if err != nil {
		return 0, context.Wrap(err, "unable to execute query")
	}

	if res.NumberOfRecordsUpdated != nil {
		return *res.NumberOfRecordsUpdated, nil
	}

	return  0, nil
}

func (aq *AuroraQuery) PrepareSql(query structs.Query) string {
	var sqlStr = ""

	switch aq.QueryType {
	case DELETE:
		sqlStr += generateDeleteExpression(query)
	default:
		sqlStr += generateSelectExpression(query)
	}


	if len(query.Where) != 0 || len(query.WhereQueryParameters) != 0 {
		var indexShared = 0
		for _, expression := range query.Where {
			sqlStr += generateWhereClause(expression, indexShared)
			indexShared++
		}

		for _, expression := range query.OrWhere {
			sqlStr += generateOrClause(expression)
			indexShared++
		}

		for _, queryParameter := range query.WhereQueryParameters {
			sqlStr += generateWhereParanthesis(queryParameter, "AND", indexShared)
			indexShared++
		}

		for _, queryParameter := range query.OrWhereQueryParameters {
			sqlStr += generateWhereParanthesis(queryParameter, "OR", indexShared)
			indexShared++
		}
	}

	if len(query.GroupBy) != 0 {
		sqlStr += "GROUP BY " + strings.Join(query.GroupBy, ", ") + " "
	}

	if len(query.Order) != 0 {
		sqlStr += "ORDER BY " + structs.JoinOrderBy(query.Order, ", ") + " "
	}

	if query.Limit[1] != 0 {
		sqlStr += "LIMIT " + strconv.Itoa(query.Limit[0]) + "," + strconv.Itoa(query.Limit[1])
	}

	if len(query.Having) != 0 || len(query.HavingQueryParameters) != 0 {
		var indexShared = 0
		for _, expression := range query.Having {
			sqlStr += generateHavingClause(expression, indexShared)
			indexShared++
		}

		for _, expression := range query.HavingOrCondition {
			sqlStr += generateOrClause(expression)
			indexShared++
		}

		for _, queryParameter := range query.HavingQueryParameters {
			sqlStr += generateHavingParanthesis(queryParameter, "AND", indexShared)
			indexShared++
		}

		for _, queryParameter := range query.HavingOrQueryParameters {
			sqlStr += generateHavingParanthesis(queryParameter, "OR", indexShared)
			indexShared++
		}
	}

	if aq.QueryType == SELECT {
		for _, query := range query.Union {
			sqlStr += " UNION " + aq.PrepareSql(query)
		}
	}

	return sqlStr
}

func generateSelectExpression(query structs.Query)string{
	var sqlStr string
	sqlStr = `SELECT ` + strings.Join(query.Select, ",") +
		` FROM ` + query.From + ` `

	if len(query.Join) != 0 {
		for _, join := range query.Join {
			sqlStr += generateJoinString(join)
		}
	}

	return sqlStr
}

func generateDeleteExpression(query structs.Query) string{
	return "DELETE FROM " + query.Delete + " "
}

func generateJoinString(join structs.Join) string {
	joinMethod := ""
	switch join.Type {
	case "left":
		joinMethod = "LEFT JOIN "
		break
	case "right":
		joinMethod = "RIGHT JOIN "
		break
	default:
		joinMethod = "JOIN "
	}

	var joinSrcAlias = join.SrcTable
	if strings.Contains(strings.ToLower(join.SrcTable), " as") {
		joinSrcAlias = strings.Replace(join.SrcTable[strings.LastIndex(join.SrcTable, " as")+3:], " ", "", -2)
	} else if strings.Contains(join.SrcTable, ")") {
		joinSrcAlias = strings.Replace(join.SrcTable[strings.Index(join.SrcTable, ")")+1:], " ", "", -1)
	}

	var joinTargetAlias = join.TargetTable
	if strings.Contains(strings.ToLower(join.TargetTable), " as") {
		joinTargetAlias = strings.Replace(join.TargetTable[strings.LastIndex(join.TargetTable, " as")+3:], " ", "", -2)
	} else if strings.Contains(join.TargetTable, ")") {
		joinTargetAlias = strings.Replace(join.TargetTable[strings.Index(join.TargetTable, ")")+1:], " ", "", -1)
	}
	return joinMethod + join.SrcTable + " ON " + "`" + joinSrcAlias + "`.`" + join.PrimaryKey + "` = `" + joinTargetAlias + "`.`" + join.ForeignKey + "` "
}

func generateWhereClause(expression string, index int) string {
	if index == 0 {
		return "WHERE " + expression + " "
	} else {
		return "AND " + expression + " "
	}
}

func generateHavingClause(expression string, index int) string {
	if index == 0 {
		return "HAVING " + expression + " "
	} else {
		return "AND " + expression + " "
	}
}

func generateOrClause(expression string) string {
	return "OR " + expression + " "

}

func generateConditionInParanthesis(queryParameters structs.QueryParameter, comparatorType string, comparator string, indexGlob int, omitPrefix bool) string {
	var sqlStr string
	if indexGlob == 0  {
		if !omitPrefix {
			sqlStr = " " + comparatorType + " ("
		} else {
			sqlStr = "  ("
		}
	} else {
		sqlStr = " " + comparator + " ("
	}

	var indexLoc = 0
	for _, condition := range queryParameters.WhereConditions {
		if indexLoc > 0 {
			sqlStr += " AND "
		}

		sqlStr += condition
		indexLoc++
		indexGlob++
	}

	for _, condition := range queryParameters.OrWhereConditions {
		if indexLoc > 0 {
			sqlStr += " OR "
		}

		sqlStr += condition
		indexLoc++
		indexGlob++
	}

	for _, queryParameter := range queryParameters.WhereFieldsSeparated {
		sqlStr += generateConditionInParanthesis(queryParameter, comparatorType, "AND", indexLoc, true)
		indexLoc++
		indexGlob++
	}

	for _, queryParameter := range queryParameters.OrWhereFieldsSeparated {
		sqlStr += generateConditionInParanthesis(queryParameter, comparatorType, "OR", indexLoc, true)
		indexLoc++
		indexGlob++
	}

	return sqlStr + ") "

}

func generateWhereParanthesis(queryParameters structs.QueryParameter, comparator string, indexGlob int) string {
	return generateConditionInParanthesis(queryParameters, "WHERE", comparator, indexGlob, false)
}

func generateHavingParanthesis(queryParameters structs.QueryParameter, comparator string, indexGlob int) string {
	return generateConditionInParanthesis(queryParameters, "HAVING", comparator, indexGlob, false)
}

func (aq *AuroraQuery) SetParameters(parameters map[string]interface{}) *AuroraQuery {
	aq.parameters = parameters
	return aq
}
