package aurora

import (
	structs "github.com/mmatagrin/sql-builder/structs"
)

type AuroraQueryBuilder struct {
	query structs.Query
}

func CreateQueryBuilder() *AuroraQueryBuilder {
	aqb := AuroraQueryBuilder{}
	return &aqb
}
/**
	do not use * otherwise the parsing of the results will fail
 */
func (aqb *AuroraQueryBuilder) Select(fields ...string) *AuroraQueryBuilder {
	aqb.query.Select = fields
	return aqb
}

func (aqb *AuroraQueryBuilder) From(table string) *AuroraQueryBuilder {
	aqb.query.From = table
	return aqb
}

func (aqb *AuroraQueryBuilder) Where(expression string) *AuroraQueryBuilder {
	aqb.query.Where = append(aqb.query.Where, expression)
	return aqb
}

func (aqb *AuroraQueryBuilder) AndWhere(expression string) *AuroraQueryBuilder {
	return aqb.Where(expression)
}

func (aqb *AuroraQueryBuilder) WhereParenthesis(callback func(queryParameter AuroraQueryParameter) AuroraQueryParameter) *AuroraQueryBuilder {
	aqb.query.WhereQueryParameters = append(aqb.query.WhereQueryParameters, callback(AuroraQueryParameter{}).Parameters)
	return aqb
}

func (aqb *AuroraQueryBuilder) OrWhereParenthesis(callback func(queryParameter AuroraQueryParameter) AuroraQueryParameter) *AuroraQueryBuilder {
	aqb.query.OrWhereQueryParameters = append(aqb.query.OrWhereQueryParameters, callback(AuroraQueryParameter{}).Parameters)
	return aqb
}

func (aqb *AuroraQueryBuilder) OrWhere(expression string) *AuroraQueryBuilder {
	aqb.query.OrWhere = append(aqb.query.OrWhere, expression)
	return aqb
}

func (aqb *AuroraQueryBuilder) Join(srctable string, targetTable string, primaryKey string, foreignKey string) *AuroraQueryBuilder {
	aqb.query.Join = append(aqb.query.Join, structs.InnerJoin(srctable, targetTable, primaryKey, foreignKey))
	return aqb
}

func (aqb *AuroraQueryBuilder) LeftJoin(srctable string, targetTable string, primaryKey string, foreignKey string) *AuroraQueryBuilder {
	aqb.query.Join = append(aqb.query.Join, structs.LeftJoin(srctable, targetTable, primaryKey, foreignKey))
	return aqb
}

func (aqb *AuroraQueryBuilder) RightJoin(srctable string, targetTable string, primaryKey string, foreignKey string) *AuroraQueryBuilder {
	aqb.query.Join = append(aqb.query.Join, structs.RightJoin(srctable, targetTable, primaryKey, foreignKey))
	return aqb

}

func (aqb *AuroraQueryBuilder) OrderBy(fields ...structs.OrderBy) *AuroraQueryBuilder {
	for _, field := range fields {
		aqb.query.Order = append(aqb.query.Order, field)
	}
	return aqb
}

func (aqb *AuroraQueryBuilder) Having(expression string) *AuroraQueryBuilder {
	aqb.query.Having = append(aqb.query.Having, expression)
	return aqb
}

func (aqb *AuroraQueryBuilder) HavingAnd(expression string) *AuroraQueryBuilder {
	return aqb.Having(expression)
}

func (aqb *AuroraQueryBuilder) HavingOr(expression string) *AuroraQueryBuilder {
	aqb.query.HavingOrCondition = append(aqb.query.HavingOrCondition, expression)
	return aqb
}

func (aqb *AuroraQueryBuilder) HavingParenthesis(callback func(queryParameter AuroraQueryParameter) AuroraQueryParameter) *AuroraQueryBuilder {
	aqb.query.HavingQueryParameters = append(aqb.query.HavingQueryParameters, callback(AuroraQueryParameter{}).Parameters)
	return aqb
}

func (aqb *AuroraQueryBuilder) HavingAndParenthesis(callback func(queryParameter AuroraQueryParameter) AuroraQueryParameter) *AuroraQueryBuilder {
	return aqb.HavingParenthesis(callback)
}

func (aqb *AuroraQueryBuilder) HavingOrParenthesis(callback func(queryParameter AuroraQueryParameter) AuroraQueryParameter) *AuroraQueryBuilder {
	aqb.query.HavingOrQueryParameters = append(aqb.query.HavingOrQueryParameters, callback(AuroraQueryParameter{}).Parameters)
	return aqb
}

func (aqb *AuroraQueryBuilder) GroupeBy(fields ...string) *AuroraQueryBuilder {
	for _, field := range fields {
		aqb.query.GroupBy = append(aqb.query.GroupBy, field)
	}
	return aqb
}

func (aqb *AuroraQueryBuilder) GetQuery() *AuroraQuery {
	var queryType QueryType
	if len(aqb.query.Select) > 0 {
		queryType = SELECT
	} else if aqb.query.Delete != "" {
		queryType = DELETE
	}
	return &AuroraQuery{AuroraQueryBuilder: *aqb, QueryType: queryType}
}

func (aqb *AuroraQueryBuilder) Union(builder AuroraQueryBuilder) *AuroraQueryBuilder {
	aqb.query.Union = append(aqb.query.Union, builder.query)
	return aqb
}

func (aqb *AuroraQueryBuilder) UnionCallback(callback func(builder AuroraQueryBuilder) AuroraQueryBuilder) *AuroraQueryBuilder {
	aqb.query.Union = append(aqb.query.Union, callback(AuroraQueryBuilder{}).query)
	return aqb
}

func (aqb *AuroraQueryBuilder) Delete(table string) *AuroraQueryBuilder {
	aqb.query.Delete = table
	return  aqb
}

func (aqb *AuroraQueryBuilder) Limit(limit ...int) *AuroraQueryBuilder {
	if len(limit) == 1 {
		aqb.query.Limit[0] = 0
		aqb.query.Limit[1] = limit[0]
	} else if len(limit) == 2 {
		aqb.query.Limit[0] = limit[0]
		aqb.query.Limit[1] = limit[1]
	} else {
		panic("Err, function Limit expected 1 or 2 parameters")
	}
	return aqb
}
