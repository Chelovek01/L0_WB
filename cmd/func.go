package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

func InsertData(conn *pgx.Conn, order OrderInfo) {
	err := InsertDataPayment(conn, order)
	if err != nil {
		fmt.Println(err)
	}
	uid, err := InsertDataOrder(conn, order)
	if err != nil {
		fmt.Println(err)
	}
	id, err := InsertDataDelivery(conn, order)
	if err != nil {
		fmt.Println(err)
	}
	err = InsertOrderDelivery(conn, uid, id)
	if err != nil {
		fmt.Println(err)
	}
	err = InsertDataItems(conn, order)
	if err != nil {
		fmt.Println(err)
	}
}

func InsertDataOrder(conn *pgx.Conn, order OrderInfo) (order_uid string, err error) {
	query := `
		insert into order_info
			(order_uid, track_number, entry, locale, internal_signature,customer_id, delivery_service, shardkey, sm_id, date_created,oof_shard) 
		values 
			($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		returning order_uid
		`
	if err = conn.QueryRow(context.Background(), query,
		order.OrderUid,
		order.TrackNumber,
		order.Entry,
		order.Locale,
		order.InternalSignature,
		order.CustomerId,
		order.DeliveryService,
		order.Shardkey,
		order.SmId,
		order.DateCreated,
		order.OofShard,
	).Scan(&order_uid); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			return "", pgErr
		}
	}
	return order_uid, nil
}

// Вставка данных о доставке в таблицу delivery в БД
func InsertDataDelivery(conn *pgx.Conn, order OrderInfo) (id int, err error) {
	query := `
		insert into deliveries
			(name, phone, zip, city, address, region, email) 
		values 
			($1,$2,$3,$4,$5,$6,$7)
		returning id
		`
	if err = conn.QueryRow(context.Background(), query,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email,
	).Scan(&id); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return 0, pgErr
		}
	}
	return id, nil
}

// Вставка данных о платеже в таблицу payments в БД
func InsertDataPayment(conn *pgx.Conn, order OrderInfo) (err error) {
	query := `
		insert into payments
			(transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total,
                      custom_fee) 
		values 
			($1,$2,$3,$4,$5,$6,$7, $8, $9, $10)
		`
	if err = conn.QueryRow(context.Background(), query,
		order.Payment.Transaction,
		order.Payment.RequestId,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee,
	).Scan(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return pgErr
		}
	}
	return nil
}

// Вставка данных в таблицу order_delivery для связи между таблицами order_info и delivery в БД
func InsertOrderDelivery(conn *pgx.Conn, order_uid string, id int) (err error) {
	query := `
		insert into order_delivery
			(order_uid,delivery_id)
		values 
			($1,$2)
		`
	if err = conn.QueryRow(context.Background(), query, order_uid, id).Scan(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return pgErr
		}
	}
	return nil
}

// Вставка данных о предметах в таблицу Items в БД
func InsertDataItems(conn *pgx.Conn, order OrderInfo) (err error) {
	for i := 0; i < len(order.Items); i++ {
		query := `
			insert into items
				(chrt_id,track_number,price,rid,name,sale,size,total_price,nm_id,brand,status)
			values
				($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			`
		if err = conn.QueryRow(context.Background(), query,
			order.Items[i].ChrtId,
			order.Items[i].TrackNumber,
			order.Items[i].Price,
			order.Items[i].Rid,
			order.Items[i].Name,
			order.Items[i].Sale,
			order.Items[i].Size,
			order.Items[i].TotalPrice,
			order.Items[i].NmId,
			order.Items[i].Brand,
			order.Items[i].Status,
		).Scan(); err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				fmt.Println(pgErr)
				return pgErr
			}
		}
	}

	return nil
}

// Получение данных по order_uid из БД
func GetDataByUid(conn *pgx.Conn, order_uid string) (string, error) {
	var order OrderInfo
	query := `
		select oi.*, to_jsonb(p.*) as "payment", (select jsonb_agg((to_jsonb(i.*))) from items i )  as "items",
			to_jsonb(del) as "delivery" 
		from order_info oi 
		left join payments p on p."transaction" = oi.order_uid 
		left join items i on i.track_number = oi.track_number
		join (
				select d.name,d.phone ,d.zip ,d.city ,d.address ,d.region ,d.email  
				from deliveries d
				where d.id = (
								select od.delivery_id 
								from order_delivery od 
								where od.order_uid=$1
								)
			) as del on true
		where oi.order_uid = $1
		limit 1`

	if err := conn.QueryRow(context.Background(), query, order_uid).Scan(
		&order.OrderUid,
		&order.TrackNumber,
		&order.Entry,
		&order.Locale,
		&order.InternalSignature,
		&order.CustomerId,
		&order.DeliveryService,
		&order.Shardkey,
		&order.SmId,
		&order.DateCreated,
		&order.OofShard,
		&order.Payment,
		&order.Items,
		&order.Delivery,
	); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return "", pgErr
		}
	}
	res, _ := json.Marshal(&order)
	return string(res), nil
}

// Получение всех order_uid из БД для
func GetOrderUid(conn *pgx.Conn) (slice_uid []string) {

	query := `
		select array_agg(order_uid) from order_info		
`
	err := conn.QueryRow(context.Background(), query).Scan(&slice_uid)
	if err != nil {
		fmt.Println(err)
	}
	return slice_uid
}

// Вставка невалидных данных в БД
func InsertInvalidData(conn *pgx.Conn, data string) (err error) {
	query := `insert into invalid_data(data) values ($1)`
	if err = conn.QueryRow(context.Background(), query, data).Scan(); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			fmt.Println(pgErr)
			return pgErr
		}

	}
	return nil
}
