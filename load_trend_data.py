from backend.src.db.DBContext import DBContext


def load_trend_data():
    # Reach out to me at tom@perseuss.xyz
    # if you want access to the full trend database
    db = DBContext.getTrendDatabaseSql()

    terms_df = db.load_select_query_to_df('''
    select t.term,
           t.date,
           t.rank,
           t.estimated_search_volume
    from weekly_time_range_stats_raw as s
        join terms_time_series_weekly as t
             on s.term = t.term
    where s.start_date = '2024-04-13'
      and s.end_date = '2024-08-31'
      and s.volume_growth_percent > 100
      and s.min_volume > 100
    order by term asc, date asc
    ''')

    terms_df.to_csv('./data/raw_term_sample.csv', index=False)

    term_set = set(terms_df['term'])
    term_set = {term.replace("'", "''") for term in term_set}
    term_set_sql_string = f"""('{"','".join(term_set)}')"""

    term_details_df = db.load_select_query_to_df(f'''
        select term,
               asin,
               title,
               category,
               subcategory,
               brand,
               list_price_amount,
               list_price_currency,
               image_url
        from terms_to_asin_details
        where term in {term_set_sql_string};
    ''')
    term_details_df.to_csv('./asin_details.csv', index=False)


if __name__ == '__main__':
    load_trend_data()
