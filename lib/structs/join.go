package structs

type Join struct {
	Type        string //inner, left, right....
	SrcTable    string
	TargetTable string
	PrimaryKey  string
	ForeignKey  string
}

func InnerJoin(srcTable string, targetTable string, primaryKey string, foreignKey string) Join {
	return Join{
		Type:        "inner",
		SrcTable:    srcTable,
		TargetTable: targetTable,
		PrimaryKey:  primaryKey,
		ForeignKey:  foreignKey}
}

func LeftJoin(srcTable string, targetTable string, primaryKey string, foreignKey string) Join {
	return Join{
		Type:        "left",
		SrcTable:    srcTable,
		TargetTable: targetTable,
		PrimaryKey:  primaryKey,
		ForeignKey:  foreignKey}
}

func RightJoin(srcTable string, targetTable string, primaryKey string, foreignKey string) Join {
	return Join{
		Type:        "right",
		SrcTable:    srcTable,
		TargetTable: targetTable,
		PrimaryKey:  primaryKey,
		ForeignKey:  foreignKey}
}
