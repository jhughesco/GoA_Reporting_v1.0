SELECT {cols} 
FROM ft_JobLogsFT ft
INNER JOIN CONTAINSTABLE(ft_JobLogsFT, file_stream, @SearchWord, 1000) AS fts 
  ON ft.stream_id = fts.[KEY]
WHERE ft.stream_id IN (
  SELECT stream_id 
  FROM vw_ft_Metadata 
  WHERE startDT BETWEEN @fromDate AND @toDate
);
