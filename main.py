from backend.src.db.DBContext import DBContext

def load_trend_data(name):

    # Reach out to me at tom@perseuss.xyz if you want access to the full trend database
    db = DBContext.getTrendDatabaseSql()

    raw_terms_df = db.load_select_query_to_df('''
    select t.*
    from weekly_time_range_stats_raw as s
        join terms_time_series_weekly as t
             on s.term = t.term
    where s.start_date = '2024-04-13'
      and s.end_date = '2024-08-31'
      and s.volume_growth_percent > 100
      and s.min_volume > 100
    order by term asc, date asc
    ''')

    raw_terms_df.to_csv('./raw_term_sample.txt', index=False)



if __name__ == '__main__':
    load_trend_data('PyCharm')

