# RabbitMQ

![什么是中间件](what_is_middleware.png)

**声明：**本教程参考了[java技术爱好者的博文](https://developer.aliyun.com/article/769883#slide-11)和[RabbitMQ官方文档](https://www.rabbitmq.com/tutorials/tutorial-one-go.html)，有大量内容就是官方文档的中文翻译，个人渣翻，还请谅解！
## 要学习哪些中间件？
1. 消息型中间件
	- ActiveMQ
	- RabbitMQ（开源）
	- Kafka（开源、性能最高）
	- RocketMQ（国产、半开源、不可靠）
2. 负载均衡中间价
	- Nginx（重点）
	- CDN（重点）
	- KeepAlive
	- Nginx
3. 缓存中间件
	- MemCache
	- Redis
4. 数据库中间件
	- Mycat
	- ShardingJdbc

## 学习中间件技术的方式和技巧
1. 理解中间件在项目架构中的作用，以及各中间件的底层实现
2. 可以使用一些类比的生活概念去理解中间件
3. 使用一些流程图或者脑图的方式去梳理各个中间件在架构中的作用 
4. 尝试用go技术去实现中间件的原理
5. 静下来去思考中间件在项目中设计的和使用的原因
6. 如果找到对应的替代总结方案，尝试编写博文总结类同中间件技术的对比和使用场景
7. 学会查看中间件的源码以及开开源项目和博文

## 基于消息中间件的分布式系统的架构
![为什么使用消息队列](why_mq.png)
假设在一个分布式系统中，系统A需要向系统B、C、D发送消息。传统的办法是写一段逻辑，向B、C、D中发送消息，而一旦D不需要消息了，那么需要在系统A中删除向D发送消息的相关代码；如果有新加入的系统E，那么则需要在A中添加向E发送消息的代码

在这一分布式系统中，可以加入MQ**从而达到解藕的目的**，系统A只需要发送消息（而不用管接收者是谁）到MQ，同时其他系统如果需要，从MQ中取消息即可
![同步请求](process-normal.png)
如图所示。一个客户端请求发送进来，系统A会调用系统B、C、D三个系统，同步请求的话，响应时间就是系统A、B、C、D的总和，也就是800ms
![异步请求](process-mq.png)
如果使用MQ，系统A发送数据到MQ，然后就可以返回响应给客户端，不需要再等待系统B、C、D的响应，可以大大地提高性能。对于一些非必要的业务，比如发送短信，发送邮件等等，就可以采用MQ

在这一分布式系统中，MQ起到**异步调用**的效果
![流量削峰](flow_peak_clipping.png)
如图所示，假设系统A在某一段时间请求数暴增，有5000个请求发送过来，系统A这时就会发送5000条SQL进入MySQL进行执行，MySQL对于如此庞大的请求当然处理不过来，MySQL就会崩溃，导致系统瘫痪。如果使用MQ，系统A不再是直接发送SQL到数据库，而是把数据发送到MQ，MQ短时间积压数据是可以接受的，然后由消费者每次拉取2000条进行处理，防止在请求峰值时期大量的请求直接发送到MySQL导致系统崩溃

在这一系统中，MQ起到**流量削峰**的作用

## 使用 RabbitMQ
### 安装 Erlang 语言（Mac）
Mac安装Erlang：

```shell
brew install erlang
```

### 安装 RabbitMQ 服务端
Mac安装RabbitMQ：

```shell
brew install rabbitmq
```

安装成功后，文件在路径 `/usr/local/Cellar/rabbitmq/版本号/` 下
### 启动
- 命令行启动

进入`/user/local/Cellar/rabbitmq/版本号/`，输入 `sbin/rabbitmq-server`，启动服务器，启动成功后显示：
![sbin/rabbitmq-server](bin-start.png)

- brew 启动

```shell
brew services start rabbitmq
# 停止rabbitmq：
brew services stop rabbitmq
```
### Hello, world!
在RabbitMQ的这一案例中，我们需要两个角色：消息的发送者和接受者
- 发送方

创建send.go：

首先，引入依赖

```go
import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)
```

定义一个帮助我们报告错误的工具函数：

```go
// helper function to check the return value for each amqp call
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s\n", msg, err)
	}
}
```

然后连接到本机的RabbitMQ服务（服务的默认端口为5672，用户名和密码默认guest）：

```go
conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
failOnError(err, "Failed to connect to RabbitMQ")
defer conn.Close()
```

接下来创建一个channel，它拥有做多数操作的API：

```go
ch, err := conn.Channel()
failOnError(err, "Failed to open a channel")
defer ch.Close()
```

必须要先定义一个队列，然后才能向队列中发送消息：

```go
	// declare a queue for us to send to
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "Hello, RabbitMQ!"
	// publish a message to the queue
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", body)
```

只有在队列已不存在时，`ch.QueueDeclare`才会创建新的队列

send.go完整代码：

```go
package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// helper function to check the return value for each amqp call
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s\n", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// declare a queue for us to send to
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "Hello, RabbitMQ!"
	// publish a message to the queue
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", body)
}

```

- 接收方

创建receive.go：

receive.go的依赖和工具函数和send.go大致相似：

```go
import (
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
```

设置阶段和send.go也大致相同，要注意的是，这里声明的queue需要和发送方声明的相匹配：

```go
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")
```

之所以在这里也要声明这个queue，是由于我们可能会在发布者之前启动接收者，所以我们要再次声明，以确保在从queue中消费消息前，这个队列是存在的

现在我们要让server将消息递交给我们了。由于它会异步地推送消息，我们只需要从`ch.Consume`给我们返回的管道中读取消息就可以了

```go
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s\n", d.Body)
		}
	}()

	wg.Wait()
```

send.go完整代码：

```go
package main

import (
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s\n", d.Body)
		}
	}()

	wg.Wait()
}
```
消息的发送者和接受者都已经编写好了，现在使用`go run send.go`和`go run receive.go`就可以看到结果了

## RabbitMQ 中的组成部分
从上述的`Hello, RabbitMQ!`的例子中，也可以大致体会到一些RabbitMQ的组成结构，事实上，它有这几部分：

- Broker：消息队列服务进程。此进程包含两个部分：Exchange和Queue
	- Exchange：消息队列交换机。**按一定的规则将消息路由转发到某个队列**
	- Queue：消息队列。存储消息的队列
- Producer：消息生产者。生产方客户端将消息经交换机路由发送到队列中
- Consumer：消息消费者。消费队列中存储的消息

![组成部分](internal_composition.png)

- 消息生产者连接到RabbitMQ Broker，创建connection，开启channel
- 生产者声明交换机类型、名称、是否持久化等
- 生产者发送消息，并指定消息是否持久化等属性和routing key
- exchange收到消息之后，根据routing key路由到跟当前交换机绑定的相匹配的队列里面
- 消费者监听接收到消息之后开始业务处理

注：`routing key`在`ch.PublishWithContext`时使用过，值为`q.Name`（q为队列）

## Exchange 的四种类型

从上面的工作流程可以看出，实际上有个关键的组件Exchange，因为消息发送到RabbitMQ后首先**要经过Exchange路由才能找到对应的Queue**

实际上Exchange类型有四种，根据不同的类型工作的方式也有所不同。在HelloWord例子中，我们就使用了比较简单的**Direct Exchange**，翻译就是直连交换机。其余三种分别是：**Fanout exchange**、**Topic exchange**、**Headers exchange**

### Direct Exchange

见文知意，直连交换机意思是此交换机需要绑定一个队列，要求该消息与一个特定的路由键完全匹配。简单点说就是一对一的，点对点的发送

### Fanout Exchange

这种类型的交换机需要将队列绑定到交换机上。一个发送到交换机的消息都会被转发到与该交换机绑定的所有队列上。很像子网广播，每台子网内的主机都获得了一份复制的消息。简单点说就是发布/订阅

![Fanout Exchange](fanout_exchange.png)

### Topic Exchange

主题交换机，根据给定的模式匹配队列，并将消息发布到匹配成功的队列中去

## Work Queues
Work Queue（也称为Task Queue）的主要思想就是，避免立即开始做资源密集型的任务，甚至还要等待任务做完。我们可以计划以后再做它，方法是将任务作为一个消息扔到队列里面，随后一个在后台运行的worker process会将该任务弹出并最终执行该任务。因此，当有众多worker时，任务将会分发给它们

这种思想在无法于一个短暂的HTTP请求窗口内处理完成一件复杂的任务的场景下非常有用

### 更改代码
在“实现Work Queue”的案例下，我们将在作为消息发送出去的字符串中加入`.`号，这么做是为了模拟出“将复杂任务作为消息扔到队列里”的场景。`.`号的数量决定了接收端处理该“任务”的时间（一个`.`处理一秒）

首先，稍微修改下send.go中的代码：

```go
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	// publish a message to the queue
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s\n", body)
```

这是`bodyFrom`函数：

```go
func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "Hello, RabbitMQ!"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
```

由于这个程序会将任务布置到队列中，所以我们把它命名为new_task.go

由于需要用一秒钟的时间处理一个`.`，所以旧的receive.go也需要一些改动，改动后将它命名为worker.go

```go
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s\n", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
		}
	}()

	log.Printf(" [*] Waiting for message. To exit press CTRL + C\n")
	wg.Wait()
```

### 循环调度（Round-robin dispatch）
使用Task Queue的一个好处就是能轻松实现并行工作，也可以通过增加worker的数量来实现规模扩大

现在，可以同时使用2个worker进程，来观察它们会如何工作

需要3个终端，前两个用来分别启动2 workers：

```shell
# shell 1
go run worker.go
# => [*] Waiting for messages. To exit press CTRL+C
```

```shell
# shell 2
go run worker.go
# => [*] Waiting for messages. To exit press CTRL+C
```

启动2个worker之后，再在第三个终端发布一些消息：

```shell
# shell 3
go run new_task.go First message.
go run new_task.go Second message..
go run new_task.go Third message...
go run new_task.go Fourth message....
go run new_task.go Fifth message.....
```

那么，这些消息具体是如何传递给workers的？

```shell
# shell 1
go run worker.go
# => [*] Waiting for messages. To exit press CTRL+C
# => [x] Received 'First message.'
# => [x] Received 'Third message...'
# => [x] Received 'Fifth message.....'
```

```shell
# shell 2
go run worker.go
# => [*] Waiting for messages. To exit press CTRL+C
# => [x] Received 'Second message..'
# => [x] Received 'Fourth message....'
```

由2个worker的输出，可知：默认情况下，RabbitMQ会将一个消息按次序送到下一个消费者那里。因此，平均下来每个消费者会得到差不多相同数量的消息。这种分配消息的方式被称为循环调度

### 消息确认
一个任务可能需要几秒钟才能完成，那么如果任务没完成，任务的接收者就被关停了怎么办？

在当前情况下，一旦RabbitMQ将消息传递给了消费者，他会立刻将消息标记为“deletion”。在这种情况下，如果关停了worker，它正在处理的消息就丢失了

RabbitMQ支持消息确认（message acknowledgments）以确保消息不回丢失。消息消费者可以将确认发回RabbitMQ，从而告知RabbitMQ可以删除特定的消息了

如果一个消费者掉线了（默认30秒之内没有返回确认），RabbitMQ就会明白任务没被处理完成，回将任务重新入队，并传递给其他消费者

另外，可以[修改](https://www.rabbitmq.com/consumers.html#acknowledgement-timeout)默认30秒的timeout

要手动确认，需要`ch.Consume`时，将auto-ack参数设置为false，并每次在worker处理完一个任务时，让它通过`d.Ack(false)`的方式确认单次消息传递

```go
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s\n", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
			d.Ack(false)
		}
	}()
```

> 忘记确认
> 忘记确认是一个普遍的错误，但它的后果是非常严重的。由于RabbitMQ无法释放这些消息，它会吃掉越来越多的内存（直到服务崩溃）
> 为了对这种错误进行debug，可以用`rabbitmqctl`命令来打印`message_unacknowledged`字段：
> ```
> sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged
> ```

执行结果：
![debug](msg_unacked.png)

### 消息持久化

目前消息不会因为worker挂掉而丢失了，但是它依旧会因RabbitMQ服务挂掉了而丢失

需要做两件事来保证当RabbitMQ推出或挂掉时不回丢失消息：

1. 将队列声明为durable=true
2. 将消息声明为durable=true

在**发送端和接受端**将队列声明为durable：

```go
q, err := ch.QueueDeclare(
	"hello", // name
	true, // durable
	false, // delete when unused
	false, // exclusive
	false, // no-wait
	nil, // arguments
)
failOnError(err, "failed to declare a queue")
```

将消息标记为`amqp.Persistent`：

```go
err = ch.PublishWithContext(ctx,
	"",           // exchange
	q.Name,       // routing key
	false,        // mandatory
	false,
	amqp.Publishing {
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(body),
})
```

> 将消息标记为persistent并不能完全保证消息不回丢失，但也足够应对简单任务队列的需求了，如果需要消息不丢失的强保障，可以查看[发布者确认](https://www.rabbitmq.com/confirms.html)

### 公平调度

在许多案例中，循环调度会导致任务分配不均，在一些情况下，某些workers的任务会一直非常简单，而另一些的任务会十分艰巨。这是由于RabbitMQ仅仅在有消息进入队列时分配消息，它不会查看从消费者的消息确认数量，处于一个“盲分配”的状态

为了克服这个问题，我们可以将`prefetch count`设置为1，这告诉了RabbitMQ一个worker一次最多抓取一个任务，不要在它没有完成前一个任务前，分配给它下一个任务

可以想象，这样一来，就会形成类似于“任务池”的效果，领到艰巨任务的进程在完成该任务前，不会再领到其他任务；领到简单任务的进程会很快再次接收其他任务，各个进程的任务量得到了平衡

```go
err = ch.Qos(
	1,     // prefetch count
	0,     // prefetch size
	false, // global
)
```

> 如果所有的worker都在忙，那么队列有可能会被占满。这种情况需要被特别注意，要解决的话可以多加几个worker，或使用其他策略

### 最终代码

最终的new_task.go：

```go
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s\n", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s\n", body)
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
```

最终的worker.go：

```go
package main

import (
	"bytes"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s\n", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done\n")
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL + C")

	wg.Wait()
}
```

## 发布/订阅（Publish/Subscribe）
在“Work Queue”一节中，我们建立了一个work queue，它假设一个消息会被传递给一个消费者，在这一节，我们需要将一个消息传递给多个消费者。这种模式就叫做“发布/订阅”

为了解释这种模式，我们需要建立一个简单的日志系统，该日志系统由发送日志消息的发送者和接受并打印日志的接收者组成。为了达到“一个接收者将日志打印到屏幕上，另一个接收者的输出重定向到文件里”的效果，我们需要让每个运行着的接收者都接收到发送者发送的每一条日志

总而言之，发布出去的日志消息会被广播到所有的接收者那里

### 交换机（Exchanges）
在上一节中我们向队列中发送消息，也从队列中取消息。这一节中则要介绍Rabbit的整个**消息模型**

我们先迅速回顾一下之前介绍的内容：

- 生产者：发送消息的用户程序
- 队列：存储消息的缓冲区
- 消费者：接收消息的用户程序

RabbitMQ的消息模型的核心理念就是生产者从不将消息直接发送进队列。实际上，生产者甚至经常都不知道消息是否被传递到队列里去了

作为替代，生产者只会将消息发送到`交换机`，交换机的一头会“吃掉”生产者的消息，另一头将刚吃消息“吐”到队列里去，就这么简单。不管是将消息添加到特定的队列末，或是将它加到很多队列的末尾，又或是将其抛弃，交换机知道如何对待它接收到的消息，具体如何对待消息是由交换机的类型决定的（具体请查看“Exchange 的四种类型以及用法”一节中的内容）

### 使用 Fanout Exchange
现在，我们可以自己声明交换机了：

```go
	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-delete
		false,    // internal
		false,    // no-wait
		nil,      // args
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(
		ctx,    // context
		"logs", // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
```

> **交换机列表**
> 
> 执行`rabbitmqctl`神器来列出交换机：
> 
> ```shell
> sudo rabbitmqctl list_exchanges
> ```

### 临时队列
之前，我们使用自己命名的队列（hello和task_queue）。

但是日志系统不回这样做。第一，我们想要获得所有的日志消息，而不是这些消息的一个子集；第二，我们只关心最新即时的日志消息，而不是那些旧的。因此：

1. 当连接上RabbitMQ时，我们需要再添加一个新的空队列。为了实现这一点（而不遇到两个队列被命名为同一个名称的错误）我们可以创建一个随机名称的队列，甚至，更好的办法是，让RabbitMQ服务替我们挑选一个随机名称
2. 一旦关闭消费者进程，它添加的队列就立刻被删除掉

以下是创建随机命名的非持久化队列的方法：

```go
q, err := ch.QueueDeclare(
	"",    // queue name
	false, // durable
	false, // auto-delete	
	true,  // exclusive
	false, // no-wait
	nil,   // args
)
```

当该方法返回时，队列实例会拥有一个由RabbitMQ生成的随机队列名。像`amq.gen-JzTY20BRgKO-HjmUJj0wLg`这样

由于声明了`exclusive=true`，队列会在连接断开时被删除

### 绑定
![绑定](bindings.png)

我们已经有了一个fanout exchange和一个临时队列，现在需要告诉交换机将消息送到刚才定义的临时队列里头去

```go
err = ch.QueueBind(
	q.Name, // queue name
	"",         // routing key
	"logs",    // exchange
	false,
	nil,
)
```

> 绑定列表
> 
> 以下是列出绑定关系列表的命令：
> 
> ```shell
> rabbitmqctl list_bindings
> ```

### 放到一起去
发送消息的生产者程序和之前的生产者之间的差别不是很大，最重要的一处改动就是现在消息会被发送到`logs`交换机中，而不是无名的默认交换机

以下是`emit_log.go`（生产者）的完整源代码：

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s\n", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Fail to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Fail to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-delete
		false,    // internal
		false,    // no-wait
		nil,      // args
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(
		ctx,    // context
		"logs", // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")

	fmt.Printf(" [x] sent %s\n", body)
}

func bodyFrom(args []string) string {
	if len(args) < 2 {
		return "hello"
	}
	return strings.Join(args[1:], " ")
}
```

鉴于向不存在的交换机发送消息是非法的，所以必须在建立连接后声明交换机

当没有队列绑定到交换机时，交换机接收到的消息会被丢弃。这是`receive_log.go`的源代码：

```go
package main

import (
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err.Error())
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to declare a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // kind
		true,     // durable
		false,    // auto-delete
		false,    //internal
		false,    // no-wait
		nil,      // args
	)
	failOnError(err, "Failed to declare a exchange")

	q, err := ch.QueueDeclare(
		"",    // queue name
		false, // durable
		false, // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		"logs", // exchange name
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to bind queue to exchange")

	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer
		true,   // auto-ack
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] is running...")
	wg.Wait()
}
```

## 路由
在“发布/订阅”一节当中，我们建立了一个日志系统，它会将所有的日志消息广播到许多接收者那里去

在这一节中，我们要给它加一个feature：要让接收者可以仅订阅一部分消息。比如，可以仍将所有日志打印到屏幕，但仅仅将严重的error记录到文件

### 绑定
绑定是交换机和队列之间的一种关系，也可以理解为：如一队列绑定了一交换机，则该队列对来自该交换机的消息感兴趣

绑定方法可以接受名为`routing_key`的参数，为了避免和`Channel.Publish`的参数搞混，以后还是叫它`binding key`（绑定键）好了。以下是创建带“绑定键”的绑定的方法：

```go
err = ch.QueueBind(
	q.Name, // queue name
	"black", // binding key
	"logs", // exchange
	false,
	nil,
)
```

绑定键的含义取决于交换机的类型。我们之前使用的`fanout exchange`就会简单地忽视绑定键的值。

### 直连交换机
我们不使用`fanout exchange`来实现过滤广播消息的需求，因为它没有提供多大的灵活性，仅仅只会无意识地广播

倒是可以使用`direct exchange`来作为替代；`direct exchange`背后的算法非常简单：消息会被投放到拥有和它的`routing key`完全匹配的`binding key`的队列中

![直连交换机设置](direct-exchange-setup.png)

在上图所示的设定中，`routing_key=orange`的消息会被路由到Q1，`routing_key=black`或`routing_key=green`的消息会被路由到Q2

### 多重绑定
![多重绑定](direct-exchange-multiple.png)

将多个队列的绑定键设置为相同值的操作是完全合法的。在上图所述的情况下，一个`routing_key=black`的消息会像`fanout exchange`的广播那样，一并发送给Q1和Q2

### 发送日志
日志发送端需要定义一个直连交换机，并将消息发送其上

```go
	err = ch.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-delete
		false,         // internal
		false,         // no-wait
		nil,           // args
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(
		ctx,                   // context
		"logs_direct",         // exchange
		severityFrom(os.Args), // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to emit the log")
```

### 接收日志
日志接收端定义队列，并将其绑定到交换机上：

```go
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	if len(os.Args) < 2 {
		log.Printf("Usage: %s [info] [warning] [error]", os.Args[0])
		os.Exit(-1)
	}

	for _, s := range os.Args[1:] {
		log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, "logs_direct", s)
		err = ch.QueueBind(
			q.Name,        // queue name
			s,             // routing key
			"logs_direct", // exchange
			false,
			nil,
		)
		failOnError(err, "Failed to bind a queue")
	}
```

### 完整代码
发送端完整代码：

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-delete
		false,         // internal
		false,         // no-wait
		nil,           // args
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(
		ctx,                   // context
		"logs_direct",         // exchange
		severityFrom(os.Args), // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to emit the log")

	fmt.Printf(" [x] sent %s\n", body)
}

func bodyFrom(argv []string) string {
	if len(argv) < 2 {
		return "hello"
	}
	return strings.Join(argv[1:], " ")
}

func severityFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "info"
	} else {
		s = os.Args[1]
	}
	return s
}
```

接收端完整代码：

```go
package main

import (
	"log"
	"os"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	if len(os.Args) < 2 {
		log.Printf("Usage: %s [info] [warning] [error]", os.Args[0])
		os.Exit(-1)
	}

	for _, s := range os.Args[1:] {
		log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, "logs_direct", s)
		err = ch.QueueBind(
			q.Name,        // queue name
			s,             // routing key
			"logs_direct", // exchange
			false,
			nil,
		)
		failOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a queue")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	wg.Wait()
}
```

## 模式（Topics）
In the previous tutorial we improved our logging system. Instead of using a fanout exchange only capable of dummy broadcasting, we used a direct one, and gained a possibility of selectively receiving the logs.

在前一节（路由）中我们改进了我们的日志系统。作为只能模拟广播的`fanout exchange`的替代，我们使用了直连交换机，并由此获得了选择性接收日志的可能性

Although using the direct exchange improved our system, it still has limitations - it can't do routing based on multiple criteria.

尽管直连交换机的使用改进了我们的系统，它依旧有一些限制——它不能基于多种规则进行路由

In our logging system we might want to subscribe to not only logs based on severity, but also based on the source which emitted the log. You might know this concept from the syslog unix tool, which routes logs based on both severity (info/warn/crit...) and facility (auth/cron/kern...).

在我们的日志系统里，我们也会希望不仅仅基于重要性（error/warning/info）来订阅日志，也可能希望基于日志的来源来进行订阅

为了实现这一需求，我们必须了解模式交换机（topic exchange）

### 模式交换机
被送往模式交换机的消息都没有明确的`routing key`，它们的`routing key`必须是一系列由点分割的单词。这些单词可以是任意的，但它们通常指定了与消息相关的特征。举几个例子：`stock.usd.nyse`，`nyse.vmw`，`quick.orange.rabbit`。`routing key`的最大的大小为255字节

可以用下面一个案例来讲述模式交换机的用法：

![模式交换机](topic-exchange.png)

在这个案例中，我们发送用于描述动物的消息，消息必须同包含三个单词（两个点）的`routing key`一起被送出。第一个单词描述速度，第二个为颜色，第三个描述物种：`<speed>.<colour>.<species>`

如图，我们进行三次绑定：Q1与`*.orange.*`绑定，Q2与`*.*.rabbit`和`lazy.#`绑定

其中，占位符的含义：

- `*` 代表一个单词
- `#` 代表零个或多个单词

也就是说：

- Q1对所有橘色（colour=orange）动物感兴趣
- Q2对兔子和慢速动物（species=rabbit ｜ speed=lazy）感兴趣

`routing key`为`quick.orange.rabbit`的消息将会被送到两个队列，而`quick.orange.fox`只会送到Q1

### 完整代码
我们的`routing key`将由两个单词组成：`<设备>.<重要性>`。代码几乎与上一节一摸一样：

发送端`emit_log_topic.go`：
```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // kind
		true,         // durable
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(
		ctx,
		"logs_topic",            // exchange
		routingKeyFrom(os.Args), // routing key
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "Failed to publish a message")

	fmt.Printf(" [x] sent %s\n", body)
}

func bodyFrom(argv []string) string {
	if len(argv) < 2 {
		return "hello"
	}
	return strings.Join(argv[1:], " ")
}

func routingKeyFrom(argv []string) string {
	if len(argv) < 4 {
		return "nil.nil"
	}
	return fmt.Sprintf("%s.%s", argv[1], argv[2])
}
```

接收端：

```go
package main

import (
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	if len(os.Args) < 2 {
		log.Printf("Usage: %s [binding_key]...", os.Args[0])
		os.Exit(0)
	}
	for _, s := range os.Args[1:] {
		log.Printf("Binding queue %s to exchange %s with routing key %s",
			q.Name, "logs_topic", s)
		err = ch.QueueBind(
			q.Name,       // queue name
			s,            // routing key
			"logs_topic", // exchange
			false,
			nil)
		failOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
```

## RPC
在Work Queue一节中，我们用多个worker进程来完成消耗时间的工作。但是，worker进程并不会返回任务完成的结果，那么如何调用远端一计算机上的某个函数，并获得它返回的结果呢？

这就需要了解远端过程调用（Remote Procedure Call or RPC）

在本节中，我们要用RabbitMQ建立一个RPC系统，包括：一个客户端和一个可扩展的RPC服务，该RPC服务会执行“计算并返回斐波那契数列”这一动作来模拟真实生产环境中我们需要处理的复杂任务

> **关于RPC**
> 
> 虽然在计算领域，RPC已经是被普遍使用的模式了，但这种模式依旧常遭指责：当程序员不知道一个函数是本地函数还是相对缓慢的RPC函数时就容易出现像“在不可预料的计算环境中得到不可信的结果”、“对debugging环节增加不必要的压力”这样的问题。对RPC的错误使用最终可能导致产生一堆屎山代码
> 
> 为了避免上述问题，就得时刻记得这三条免费建议：
> 
> - 确保本地函数和RPC函数之间泾渭分明
> - 写好注释和文档，确保依赖关系明确
> - 对错误发生的情况进行处理：如果RPC服务长时间掉线，客户端会如何反应？
> 
> 疑之不用。如果可以，宁可使用异步管道来替代RPC

### 回调队列
总而言之利用RabbitMQ来建立RPC相对容易：客户端将请求消息送出，服务回复它一个响应消息。为了收到响应消息，我们需要将“回调”队列的地址与请求消息一同送出：

```go
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"", // exchange
		"rpc_queue", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			CorrelationId: corrId,
			ReplyTo: q.Name,
			Body: []byte(strconv.Itoa(n)),
		},
	)
```

> **属性管理**
> 
> AMQP 0-9-1协议定义了14个可与消息一同发出的属性。其中多数属性基本用不上，除了这几个：
> 
> - persistent：值为true时将消息标记为持久化（关于“持久化”的内容，详见“Work Queue”一节）
> - content-type：描述编码格式，例如`text/plain`或`application/json`
> - reply-to：回调队列
> - correlation-id：将RPC响应与请求相关联

### Correlation Id
前面所讲的方法，是为每个RPC请求创建一个回调队列。这确实不怎么高效，但是好在，我们也可以选择仅为每个客户端创建一个回调队列

这就引出了一个问题，假设队列里收到了一个响应，如何判断该响应属于哪个请求呢？这就轮到`correlation-id`发挥作用了！我们为每个请求的`correlation-id`属性设置一个独一无二的值，等之后我们在回调队列里一个消息时，检查它的`correlation-id`属性，基于这个来将请求和响应一一匹配。如果发现了有未知`correlation-id`值的消息，大可放心地删除该消息——它不属于我们的请求

### 总结
![RPC系统](rpc-system.png)

我们的RPC工作方式：

- 当客户端启动，它来创建一个匿名独享的回调队列
- 对每一个RPC请求，客户端为消息设置好两个属性：`reply-to`，其值为刚创建的回调队列、`correlation-id`，其值独一无二
- 请求会被送到`rpc_queue`队列
- RPC worker（aka：server）等待请求进入`rpc_queue`，当有请求进入时，完成工作并将结果通过请求的`reply-to`属性指定的队列送回到客户端
- 客户端等待数据进入回调队列。当消息出现时，检查它的`correlation_id`属性，如果其值匹配，将数据返回给应用

### 代码
斐波那契函数：

```go
func fib(n int) int {
	if n <= 0 {
		return 0
	} else if n <= 2 {
		return 1
	}
	
	dp := make([]int, 2)
	dp[0] = 1
	dp[1] = 1
	
	for i := 2; i < n; i ++ {
		dp[0], dp[1] = dp[1], dp[0] + dp[1]
	}
	
	return dp[1]
}
```

RPC server的代码长这样：

```go
package main

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func fib(n int) int {
	if n <= 0 {
		return 0
	} else if n <= 2 {
		return 1
	}

	dp := make([]int, 2)
	dp[0] = 1
	dp[1] = 1

	for i := 2; i < n; i++ {
		dp[0], dp[1] = dp[1], dp[0]+dp[1]
	}

	return dp[1]
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_queue", // request queue
		false,       // durable
		false,       // auto-delete
		false,       // exclusive
		false,       // no-wait
		nil,         // args
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1, // prefetch-count
		0, // prefetch-size
		false,
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // request queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for d := range msgs {
			n, err := strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")

			log.Printf(" [.] fib(%d)", n)
			response := fib(n)

			err = ch.PublishWithContext(ctx,
				"",        // exchange
				d.ReplyTo, // callback queue
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(strconv.Itoa(response)),
				},
			)
			failOnError(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")
	wg.Wait()
}
```

RPC server的代码比较直白：

- 日常建立connection、channel，并声明队列
- 我们有可能会运行多个服务进程，为了多个服务间的负载均衡我们需要设置channel的`prefetch`
- 调用`Channel.Consume`获得一个go channel，以便从中获取消息，随后开启完成任务并返回响应的协程

RPC Client的源代码：

```go
package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func fibonacciRPC(n int) (res int, err error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	corrId := randomString(32)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",          // exchange
		"rpc_queue", // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(strconv.Itoa(n)),
		},
	)

	for d := range msgs {
		if corrId == d.CorrelationId {
			res, err = strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to interger")
			break
		}
	}

	return
}

func main() {
	rand.Seed(time.Now().UnixNano())

	n := bodyFrom(os.Args)

	log.Printf(" [x] Requesting fib(%d)", n)
	res, err := fibonacciRPC(n)
	failOnError(err, "Failed to handle RPC Request")

	log.Printf(" [.] Got %d", res)
}

func bodyFrom(argv []string) int {
	var s string
	if (len(argv) < 2) || os.Args[1] == "" {
		s = "30"
	} else {
		s = strings.Join(argv[1:], " ")
	}
	n, err := strconv.Atoi(s)
	failOnError(err, "Failed to convert arg to integer")
	return n
}
```
