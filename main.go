package main

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"go.mercari.io/datastore"
	"go.mercari.io/datastore/boom"
	"go.mercari.io/datastore/clouddatastore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"log"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"
)

type DummyObject struct {
	ID   string `datastore:"-" boom:"id"`
	Parent datastore.Key
	Name string
}

type DummyObject2 struct {
	ID   string `datastore:"-" boom:"id"`
	Name string
}

type DummyObject3 struct {
	ID   string `datastore:"-" boom:"id"`
	Name string
}

func main() {
	var err error
	ctx := context.Background()
	b := CreateBoom(ctx)

	err = BenchFuncSeconds(PutMultiWithTxBoom, b)
	if err != nil {
		log.Printf("PutMultiWithTxBoom err: %v", err)
	}

	err = BenchFuncSeconds(PutMultiWithTxBoom2, b)
	if err != nil {
		log.Printf("PutMultiWithTxBoom err: %v", err)
	}

	err = BenchFuncSeconds(PutMultiWithTx, b)
	if err != nil {
		log.Printf("PutMultiWithTx err: %v", err)
	}

	err = BenchFuncSeconds(PutMultiWithWaitGroup, b)
	if err != nil {
		log.Printf("PutMultiWithWaitGroup err: %v", err)
	}

	err = BenchFuncSeconds(PutMultiWithErrorGroupTx, b)
	if err != nil {
		log.Printf("PutMultiWithErrorGroupTx err: %v", err)
	}

	err = BenchFuncSeconds(PutMultiWithErrorGroupBoom, b)
	if err != nil {
		log.Printf("PutMultiWithErrorGroupBoom err: %v", err)
	}

	err = BenchFuncSeconds(CreateDatastoreKeies, b)
	if err != nil {
		log.Printf("CreateDatastoreKeies err: %v", err)
	}

	err = BenchFuncSeconds(GetFirstWithIterator, b)
	if err != nil {
		log.Printf("GetFirstWithIterator err: %v", err)
	}

	err = BenchFuncSeconds(GetFirstWithGetAll, b)
	if err != nil {
		log.Printf("GetFirstWithGetAll err: %v", err)
	}

	err = BenchFuncSeconds(GetMultiWithIterator, b)
	if err != nil {
		log.Printf("GetWithIterator err: %v", err)
	}
}

func GetDoList() []*DummyObject {
	return []*DummyObject{
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
	}
}

func GetDoList2() []*DummyObject2 {
	return []*DummyObject2{
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
	}
}

func GetDoList3() []*DummyObject3 {
	return []*DummyObject3{
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
		{ID: uuid.New().String(), Name: "Hoge"},
	}
}

func CreateBoom(ctx context.Context) *boom.Boom {
	credentialPath := os.Getenv("CREDENTIAL_FILE_PATH")
	opts := datastore.WithCredentialsFile(credentialPath)
	cli, _ := clouddatastore.FromContext(ctx, opts)
	return boom.FromClient(ctx, cli)
}

func PutMultiWithTxBoom(b *boom.Boom) error {
	if _, err := b.RunInTransaction(func(tx *boom.Transaction) error {
		doList := GetDoList()
		_, err := tx.Boom().PutMulti(doList)
		if err != nil {
			return err
		}
		doList2 := GetDoList2()
		_, err = tx.Boom().PutMulti(doList2)
		if err != nil {
			return err
		}
		doList3 := GetDoList3()
		_, err = tx.Boom().PutMulti(doList3)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func PutMultiWithTxBoom2(b *boom.Boom) error {
	if _, err := b.RunInTransaction(func(tx *boom.Transaction) error {
		bm := tx.Boom()
		doList := GetDoList()
		_, err := bm.PutMulti(doList)
		if err != nil {
			return err
		}
		doList2 := GetDoList2()
		_, err = bm.PutMulti(doList2)
		if err != nil {
			return err
		}
		doList3 := GetDoList3()
		_, err = bm.PutMulti(doList3)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func PutMultiWithTx(b *boom.Boom) error {
	_, err := b.RunInTransaction(func(tx *boom.Transaction) error {
		doList := GetDoList()
		_, err := tx.PutMulti(doList)
		if err != nil {
			return err
		}
		doList2 := GetDoList2()
		_, err = tx.PutMulti(doList2)
		if err != nil {
			return err
		}
		doList3 := GetDoList3()
		_, err = tx.PutMulti(doList3)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func PutMultiWithWaitGroup(b *boom.Boom) error {
	_, _ = b.RunInTransaction(func(tx *boom.Transaction) error {
		funcs := []func(){
			func() {
				doList := GetDoList()
				tx.PutMulti(doList)
			},
			func() {
				doList2 := GetDoList2()
				tx.PutMulti(doList2)
			},
			func() {
				doList3 := GetDoList3()
				tx.PutMulti(doList3)
			},
		}

		var waitGroup sync.WaitGroup
		for _, f := range funcs {
			waitGroup.Add(1)
			go func(function func()) {
				defer waitGroup.Done()
				function()
			}(f)
		}
		waitGroup.Wait()
		return nil
	})
	return nil
}

func PutMultiWithErrorGroupTx(b *boom.Boom) error {
	_, err := b.RunInTransaction(func(tx *boom.Transaction) error {
		funcs := []func() error {
			func() error {
				doList := GetDoList()
				_, err := tx.PutMulti(doList)
				if err != nil {
					return err
				}
				return nil
			},
			func() error {
				doList2 := GetDoList2()
				_, err := tx.PutMulti(doList2)
				if err != nil {
					return err
				}
				return nil
			},
			func() error {
				doList3 := GetDoList3()
				_, err := tx.PutMulti(doList3)
				if err != nil {
					return err
				}
				return nil
			},
		}
		eg := errgroup.Group{}
		for _, f := range funcs {
			eg.Go(func() error {
				err := f()
				if err != nil {
					return err
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
		return nil
	})
	return err
}

func PutMultiWithErrorGroupBoom(b *boom.Boom) error {
	_, err := b.RunInTransaction(func(tx *boom.Transaction) error {
		bm := tx.Boom()
		funcs := []func() error {
			func() error {
				doList := GetDoList()
				_, err := bm.PutMulti(doList)
				if err != nil {
					return err
				}
				return nil
			},
			func() error {
				doList2 := GetDoList2()
				_, err := bm.PutMulti(doList2)
				if err != nil {
					return err
				}
				return nil
			},
			func() error {
				doList3 := GetDoList3()
				_, err := bm.PutMulti(doList3)
				if err != nil {
					return err
				}
				return nil
			},
		}
		eg := errgroup.Group{}
		for _, f := range funcs {
			eg.Go(func() error {
				err := f()
				if err != nil {
					return err
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return err
		}
		return nil
	})
	return err
}

func GetFirstWithIterator(b *boom.Boom) error {
	var do DummyObject
	var err error
	_, _ = b.RunInTransaction(func(tx *boom.Transaction) error {
		bm := tx.Boom()
		q := bm.NewQuery("DummyObject")
		it := bm.Run(q)
		_, err = it.Next(&do)
		if err != nil {
			if err == iterator.Done {
				return errors.New("not found")
			}
			return err
		}
		return nil
	})
	log.Println(do)
	return nil
}

func GetFirstWithGetAll(b *boom.Boom) error {
	var do *DummyObject
	_, _ = b.RunInTransaction(func(tx *boom.Transaction) error {
		bm := tx.Boom()
		q := bm.NewQuery("DummyObject").
				Offset(1)
		var doList []*DummyObject
		count, _ := bm.Count(q)
		if count == 0 {
			return errors.New("not found")
		}
		_, _ = bm.GetAll(q, &doList)
		do = doList[0]
		return nil
	})
	return nil
}

func GetMultiWithIterator(b *boom.Boom) error {
	var doList []*DummyObject
	var err error
	_, _ = b.RunInTransaction(func(tx *boom.Transaction) error {
		bm := tx.Boom()
		q := bm.NewQuery("DummyObject")
		it := bm.Run(q)
		for i:=0;i<3;i++ {
			var do DummyObject
			_, err = it.Next(&do)
			if err != nil {
				break
			}
			doList = append(doList, &do)
		}
		if err != nil && err != iterator.Done {
			return err
		}
		return nil
	})
	return nil
}

func CreateDatastoreKeies(b *boom.Boom) error {
	_, _ = b.RunInTransaction(func(tx *boom.Transaction) error {
		for i:=0;i<100;i++ {
			parent := tx.Key(&DummyObject2{ID: uuid.New().String()})
			_ = tx.Key(&DummyObject{ID: uuid.New().String(), Parent: parent})
		}
		return nil
	})
	return nil
}

func BenchFuncSeconds(f func(b *boom.Boom) error, b *boom.Boom) error {
	st := time.Now()
	err := f(b)
	ed := time.Now()
	fv := reflect.ValueOf(f)
	funcName := runtime.FuncForPC(fv.Pointer()).Name()
	if err != nil {
		log.Printf("%v - %s - faild. err: %v", ed.Sub(st), funcName, err)
	}
	log.Printf("%v - %s", ed.Sub(st), funcName)
	return nil
}
