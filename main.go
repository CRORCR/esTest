package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"io"
	"log"
	"strings"
	"time"
)

var client *elasticsearch.Client

func init() {
	addresses := []string{"http://127.0.0.1:9200"} // 可以配置集群
	config := elasticsearch.Config{
		Addresses: addresses,
		//Username:  "esName",
		//Password:  "pass",
		CloudID: "",
		APIKey:  "",
	}
	var err error
	client, err = elasticsearch.NewClient(config)
	if err != nil {
		fmt.Println("连接错误,err:", err)
	}
	log.Println("version:", elasticsearch.Version)
}

func main() {
	ctx := context.Background()

	// 保存
	user := UserInfo{Id: 5, Name: "nht", Address: "浙江湖州", Age: 30, Sex: 1, RegisteredAt: time.Now().Unix()}
	if err := saveUser(ctx, user); err != nil {
		fmt.Println(err)
	}

	// 删除
	if err := delUser(ctx, 10); err != nil {
		fmt.Println(err)
	}

	// 查询指定id select * from info where id=?
	getUserById(ctx, 4)

	// 查询其他条件 select * from info where name=?
	getUserByName(ctx, "lcq2")

	// in 方式查询 select * from info where name in (lcq3,lcq4);
	getUserByNameIn(ctx)

	// between and select * from info where age between ? and ?;
	getUserByAgeBetween(ctx)

	// select * from info where age =36 and id != 3 order by age desc;
	getUserByAge(ctx)

	// 数组方式传递 select * from info where age between ? and ? and id=3
	getUserByAgeBetweenAnd(ctx)

	// select id,name,age from info where !(age >=10 and age<20) and (id=4 or name='nht') order by age desc;
	getUserByTimeBetweenV3(ctx)
}

// 保存
func saveUser(ctx context.Context, info UserInfo) (err error) {
	var buf bytes.Buffer
	if err = json.NewEncoder(&buf).Encode(info); err != nil {
		return fmt.Errorf("encoding doc err,%v", err)
	}

	marshal, _ := json.Marshal(info)

	res, err := client.Create(
		"info",
		fmt.Sprintf("%v", info.Id),
		strings.NewReader(string(marshal)),
		//strings.NewReader(buf.String()),
		client.Create.WithPretty(),
		client.Create.WithContext(ctx),
	)
	if err != nil || res.HasWarnings() {
		return fmt.Errorf("写入数据失败,err:%v", err)
	}

	defer res.Body.Close() // SKIP
	return nil
}

// 删除
func delUser(ctx context.Context, id int64) (err error) {
	res, err := client.Delete(
		"info",
		//fmt.Sprintf("%v", id),
		fmt.Sprintf("%v", "_search"),
	)
	if err != nil || res.HasWarnings() {
		return fmt.Errorf("写入数据失败,err:%v", err)
	}

	defer res.Body.Close() // SKIP
	return nil
}

// sum查询可以么？ 不行

// select * from info where id=?
func getUserById(ctx context.Context, id int64) (info *UserInfo, err error) {
	res, err := client.Get(
		"info",
		fmt.Sprintf("%v", id),
		client.Get.WithFilterPath("_source"),
		client.Get.WithPretty(),
		client.Get.WithContext(ctx),
	)

	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}

	// 数据为空
	if res.StatusCode == 404 {
		return nil, nil
	}

	// 关闭响应正文
	defer res.Body.Close()
	//fmt.Println("是啥", res.String())

	all, _ := io.ReadAll(res.Body)
	type body struct {
		Source Source `json:"_source"`
	}
	rusult := body{}
	json.Unmarshal(all, &rusult)
	userInfo := UserInfo{
		Id:           rusult.Source.Id,
		Name:         rusult.Source.Name,
		Address:      rusult.Source.Address,
		Age:          rusult.Source.Age,
		Sex:          rusult.Source.Sex,
		RegisteredAt: rusult.Source.RegisteredAt,
	}

	fmt.Println("---", userInfo)
	return &userInfo, nil
}

// select * from info where name=?
func getUserByName(ctx context.Context, name string) (info *UserInfo, err error) {
	res, err := client.Search(
		client.Search.WithIndex("info"),
		client.Search.WithQuery(fmt.Sprintf("name:%v", name)),
		client.Search.WithPretty(),
		client.Search.WithContext(ctx),
		client.Search.WithFilterPath("hits.hits._source"),
		client.Search.WithSort("registered_at:asc"), // 设置排序字段，根据Created字段升序排序，第二个参数false表示逆序
	)
	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}

	// 关闭响应正文
	defer res.Body.Close()
	fmt.Println("是啥", res.String())
	return nil, err
}

//  in 方式查询
// select * from info where name in (lcq3,lcq4);
func getUserByNameIn(ctx context.Context) (info *UserInfo, err error) {
	res, err := client.Search(
		client.Search.WithIndex("info"),
		client.Search.WithBody(strings.NewReader(`{
		  "query": {
		    "terms": {
		      "name": [
		        "lcq3",
		        "lcq4"
		      ],
		      "boost": 1.0
		    }
		  }
		}`)),
		client.Search.WithPretty(),
		client.Search.WithContext(ctx),
		client.Search.WithFilterPath("hits.hits._source"),
		client.Search.WithSort("age:desc"), // 设置排序字段，根据Created字段升序排序，第二个参数false表示逆序
	)
	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}

	// 关闭响应正文
	defer res.Body.Close()
	fmt.Println("是啥", res.String())

	all, _ := io.ReadAll(res.Body)
	result := BaseResult{}
	json.Unmarshal(all, &result)
	fmt.Println("len:", len(result.Hits.Hits))
	fmt.Println("0", result.Hits.Hits[0])
	fmt.Println("1", result.Hits.Hits[1])

	return nil, err
}

// select * from info where age between ? and ?;
func getUserByAgeBetween(ctx context.Context) (info *UserInfo, err error) {
	res, err := client.Search(
		client.Search.WithIndex("info"),
		client.Search.WithBody(strings.NewReader(`{
	"query": {
		"range": {
			"age": {
				"gt": 35,
				"lte": 36
			}
		}
	}
}`)),
		// gt:大于 gte：大于等于

		client.Search.WithPretty(),
		client.Search.WithContext(ctx),
		client.Search.WithFilterPath("hits.hits._source"),
		client.Search.WithSort("age:desc"), // 设置排序字段，根据Created字段升序排序，第二个参数false表示逆序
	)
	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}
	// 关闭响应正文
	defer res.Body.Close()
	fmt.Println("是啥", res.String())
	return nil, err
}

// 范围查询
//select * from info where age between ? and ? and id=3
func getUserByAgeBetweenAnd(ctx context.Context) (info *UserInfo, err error) {
	// map方式拼接查询条件

	// select * from info where name=lcq2;
	//var buf bytes.Buffer
	//query := map[string]interface{}{
	//	"query": map[string]interface{}{
	//		"match": map[string]interface{}{
	//			"name": "lcq2",
	//		},
	//	},
	//}

	// select * from info where age>35 and age<=36;
	//query := map[string]interface{}{
	//	"query": map[string]interface{}{
	//		"range": map[string]interface{}{
	//			"age": map[string]interface{}{
	//				"gt":  35,
	//				"lte": 36,
	//			},
	//		},
	//	},
	//}
	//if err := json.NewEncoder(&buf).Encode(query); err != nil {
	//	log.Fatalf("Error encoding query: %s", err)
	//}

	res, err := client.Search(
		client.Search.WithIndex("info"),
		//client.Search.WithBody(&buf),

		// select * from info where age >35 and age<=36 and id !=3;
		client.Search.WithBody(strings.NewReader(`
{
	"query": {
		"bool": {
			"must": {
				"range": {
					"age": {
						"gte": 35,
						"lte": 36
					}
				}
			},
			"must_not": [{
				"match": {
					"id": "3"
				}
			}]
		}
	}
}`)),
		// gt:大于 gte：大于等于

		client.Search.WithPretty(),
		client.Search.WithContext(ctx),
		client.Search.WithFilterPath("hits.hits._source"),
		client.Search.WithSort("age:desc"), // 设置排序字段，根据Created字段升序排序，第二个参数false表示逆序
	)
	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}
	// 关闭响应正文
	defer res.Body.Close()
	fmt.Println("是啥", res.String())
	return nil, err
}

// select * from info where age =36 and id != 3 order by age desc;
func getUserByAge(ctx context.Context) (info *UserInfo, err error) {
	res, err := client.Search(
		client.Search.WithIndex("info"),
		client.Search.WithBody(strings.NewReader(`{
		  "query": {
		    "bool": {
		      "must": [
		        {
		          "match": {
		            "age": "36"
		          }
		        }
		      ],
		      "must_not": [
		        {
		          "match": {
		            "id": "3"
		          }
		        }
		      ]
		    }
		  }
		}`)),
		// gt:大于 gte：大于等于

		client.Search.WithPretty(),
		client.Search.WithContext(ctx),
		client.Search.WithFilterPath("hits.hits._source"),
		client.Search.WithSort("age:desc"), // 设置排序字段，根据Created字段升序排序，第二个参数false表示逆序
	)
	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}
	// 关闭响应正文
	defer res.Body.Close()
	fmt.Println("是啥", res.String())
	return nil, err
}

// select id,name from info where name=lcq2 limit 0,1;
func getUserById2(ctx context.Context, id int64) (info *UserInfo, err error) {
	res, err := client.Search(
		client.Search.WithIndex("info"),
		client.Search.WithQuery("name:lcq2"),
		//client.Search.WithFilterPath("took,hits.hits._id,hits.hits._source"),
		client.Search.WithFilterPath("hits.hits._id,hits.hits._source"), // 查询那些字段
		client.Search.WithPretty(),
		client.Search.WithSize(1), // 分页
		client.Search.WithContext(ctx),
		client.Search.WithFrom(0), // 从哪开始 offset
		//client.Search.WithSource("name"), // 查询某些字段
	)
	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}

	// 关闭响应正文
	defer res.Body.Close()

	all, err := io.ReadAll(res.Body)
	var resu BaseResult
	err = json.Unmarshal(all, &resu)
	fmt.Println("错误:", err)
	fmt.Println("结果:", resu.Hits.Hits)
	return nil, err
}

// select id,name,age from info where !(age >=10 and age<20) and (id=4 or name='nht') order by age desc;
func getUserByTimeBetweenV3(ctx context.Context) (info *UserInfo, err error) {
	res, err := client.Search(
		client.Search.WithIndex("info"),
		client.Search.WithBody(strings.NewReader(`
{
  "query": {
    "bool" : {
      "must_not" : {
        "range" : {
          "age" : { "gte" : 10, "lte" : 20 }
        }
      },
      "should" : [
        { "term" : { "id" : 4} },
        { "term" : { "name" : "nht" } }
      ],
      "boost" : 1.0
    }
  }
}
`)),

		// gt:大于 gte：大于等于
		client.Search.WithPretty(),
		client.Search.WithContext(ctx),
		client.Search.WithFilterPath("hits.hits._source.id,hits.hits._source.name,hits.hits._source.age"),
		client.Search.WithSort("age:desc"), // 设置排序字段，根据Created字段升序排序，第二个参数false表示逆序
	)
	if err != nil {
		return nil, fmt.Errorf("连接失败,err:%v", err)
	}
	// 关闭响应正文
	defer res.Body.Close()
	fmt.Println("是啥", res.String())
	return nil, err
}

type BaseResult struct {
	Hits Hits `json:"hits"`
}

type Hits struct {
	Hits []Hit `json:"hits"`
}
type Hit struct {
	Id     string `json:"_id"`
	Source Source `json:"_source"`
}

type Source struct {
	Id           int64  `json:"id"`
	Name         string `json:"name"`
	Address      string `json:"address"`
	Age          int64  `json:"age"`
	Sex          int64  `json:"sex"`
	RegisteredAt int64  `json:"registered_at"`
}

type UserInfo struct {
	Id           int64  `json:"id"`
	Name         string `json:"name"`          // 姓名
	Address      string `json:"address"`       // 地址
	Age          int64  `json:"age"`           // 年龄
	Sex          int64  `json:"sex"`           // 1:女 2：男
	RegisteredAt int64  `json:"registered_at"` // 注册时间
}
