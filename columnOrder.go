package dbhelper

type columnOrder struct {
	colNames []string
}

func (c *columnOrder) delete(colName string) {
	iFound := -1
	for i, v := range c.colNames {
		if v == colName {
			iFound = i
			break
		}
	}
	if iFound > -1 {
		c.colNames = append(c.colNames[:iFound], c.colNames[iFound+1:]...)
	}
}
func (c *columnOrder) rename(oldName, newName string) {
	for i, v := range c.colNames {
		if v == oldName {
			c.colNames[i] = newName
			break
		}
	}
}
func (c *columnOrder) insert(prevName, name string) {
	if prevName == "" {
		c.colNames = append([]string{name}, c.colNames...)
		return
	}
	iFound := -1
	for i, v := range c.colNames {
		if v == prevName {
			iFound = i
			break
		}
	}
	if iFound > -1 {
		c.colNames = append(append(c.colNames[:iFound], name), c.colNames[iFound:]...)
	} else {
		c.colNames = append(c.colNames, name)
	}
}
func (c *columnOrder) reorder(newOrder []string) {
	rev := &columnOrder{newOrder}
	for i, oldName := range c.colNames {
		bFound := false
		for _, newName := range newOrder {
			if newName == oldName {
				bFound = true
				break
			}
		}
		if !bFound {
			if i == 0 {
				rev.insert("", oldName)
			} else {
				rev.insert(c.colNames[i-1], oldName)
			}
		}
	}
	c.colNames = rev.colNames
}
