<?

/*
* search.php
*
* Script for searching a database 
* 

*/

print "<html><head><title>My Search Engine</title></head><body>\n";

$keyword = addslashes( $_POST['keyword'] );
$array = explode(" ", $keyword);
for ($i=0;$i<sizeof($array);$i++){
	$array[$i]=strtoupper($array[$i]);
}
if( ($_POST['keyword'])&&(sizeof($array)<3) )
{
   // Connect to the database:
   // mysql -h 46.137.175.244 -P 3757 -u emdc -pemdc OR
   // mysql -h ec2-46-137-175-244.eu-west-1.compute.amazonaws.com -P 3757 -u emdc -pemdc
   mysql_connect("ec2-46-137-175-244.eu-west-1.compute.amazonaws.com:3757","emdc","emdc")
   //mysql_connect("localhost:3306","root","emdc")
       or die("ERROR: Could not connect to database!");
   mysql_select_db("cc");
   print "<form method='post'> Keyword: 
          <input type='text' size='20' name='keyword'>\n";
   print "<input type='submit' value='Search'></form>\n";

   // Get timestamp before executing the query:
   $start_time = getmicrotime();

   // Set $keyword and $results, and use addslashes() to
   //  minimize the risk of executing unwanted SQL commands:
   $results = addslashes( $_POST['results'] );

   //$result0 = ("SELECT s.name AS url, s.pagerank AS pr FROM hashTable h, sites s WHERE h.word = \"$keyword\" AND h.site_id = s.id GROUP BY s.name ORDER BY s.pagerank DESC");
   //print $result0."\n";

   print "<h2>Search results for '".$_POST['keyword']."':</h2>\n";

   $Rank = array();
   $Sites = array();
   $dProx = 0.75;
   $dOcur = 0.25;
   $max_prox=0;
   $min_prox=-2;

   $query="SELECT MAX(s.pagerank) as max, MIN(s.pagerank) as min FROM hashTable h, sites s WHERE h.word=\"$array[0]\" AND h.site_id IN (SELECT site_id FROM hashTable WHERE word=\"$array[1]\" GROUP BY site_id) AND h.site_id = s.id";
   $result = mysql_query($query);
   for( $i = 1; $row = mysql_fetch_array($result); $i++ )
   {
	$maxPR=$row['max'];
	$minPR=$row['min'];
   }

   $query = "SELECT s.name AS url, s.id AS id, s.pagerank AS pr FROM hashTable h, sites s WHERE h.word=\"$array[0]\" AND h.site_id IN (SELECT site_id FROM hashTable WHERE word=\"$array[1]\" GROUP BY site_id) AND h.site_id = s.id GROUP BY s.name, s.id ORDER BY s.pagerank DESC";
   //print "First Round: ".$query."\n";
   $result = mysql_query($query);
   
   for( $i = 1; $row = mysql_fetch_array($result); $i++ )
   {
	
      $query = "SELECT h.word AS word, h.position AS position FROM hashTable h WHERE (";
      foreach ($array as $key => $keyword){
      	$query .= "h.word = \"$keyword\"";
	if ($key != (sizeof($array) - 1)){
		$query .= " OR ";
	}else{
		$query .= ") AND h.site_id = \"$row[id]\" ORDER BY h.word, h.position ASC ";
	}
      }
      $firstWord = array();	
      $secondWord = array();	
      //print "$query.\n";
      $result2 = mysql_query($query);
      
      for( $j = 1; $row2 = mysql_fetch_array($result2); $j++ )
      {
	if ($row2['word']==$array[0]){
      		$firstWord[] = $row2['position'];
	}else{
      		$secondWord[] = $row2['position'];
	}
      }
      
      $proximity=-1;
      $div=0;
      for ($j = 0; $j < count($firstWord); $j++){
        	$sum=0;
		for($w=0;$w<count($secondWord);$w++){
			if ($firstWord[$j]<$secondWord[$w]){
				$sum+=$secondWord[$w]-$firstWord[$j];
			        $div++;	
			}
		}
		$proximity+=$sum;
      }

	      $proximity=$proximity/$div;
      if ($max_prox<$proximity){
        $max_prox=$proximity;
      }
      $Sites[] = $row['url'];
      $Rank[] = $proximity;
      $PR[] = $row['pr']; 
      $ids[] = $row['id'];
   }

   $factor= $max_prox/$maxP;
   $new_max=0;
   for ($i=0;$i<count($Rank);$i++){
        $Rank[$i]=$Rank[$i]/$factor;
	if ($Rank[$i]>$new_max){
		$new_max=$Rank[$i];
	}
   }
   for ($i=0;$i<count($Rank);$i++){
        $Rank[$i]=$PR[$i]*(1-$dProx) + ($new_max-$Rank[$i])*$dProx;
   }
   
   $Rank = shellSort($Rank,$Sites);
   for ($i=count($Rank)-1;$i>=0;$i--){
      print "<a href='".$Sites[$i]."'>".$Sites[$i]."</a>\n";
      print "(Rank proximity: ".$Rank[$i].")<br><br>\n";
   }

   $query = "SELECT s.name AS url, s.pagerank AS pr, COUNT(h.position) AS ocurrences FROM hashTable h, sites s WHERE (";
   foreach ($array as $key => $keyword){
   $query .= "h.word = \"$keyword\"";
	if ($key != (sizeof($array) - 1)){
		$query .= " OR ";
	}else{
   	      if (count($array)>1){
		$query .= ") AND h.site_id = s.id AND h.site_id NOT IN (";
	      }else{
		$query .=") AND h.site_id = s.id GROUP BY s.name ORDER BY s.pagerank DESC";

	      }
	}
   }
   for ($i=0;$i<count($ids);$i++){ 
 	if ($i==count($ids)-1){
		$query.=$ids[$i].") GROUP BY s.name ORDER BY s.pagerank DESC ";
	}else{
		$query.=$ids[$i].", ";
  	}
   }
   //print "Second Round query.$query.\n";
   $result = mysql_query($query);

   $Sites = array();
   $Rank = array();
   $max_occ=0;
   for( $i = 1; $row = mysql_fetch_array($result); $i++ )
   {
      $Sites[] = $row['url'];  
      $Rank[] = $row['ocurrences'];  
      $PR[] = $row['pr'];
      if ($max_occ<$row['ocurrences']){
	$max_occ=$row['ocurrences'];
      }  
      if ($maxPR<$row['pr']){
	$maxPR=$row['pr'];
      }  
   }
   $factor=$max_occ/$maxPR;
   $new_max=0;
   for ($i=0;$i<count($Rank);$i++){
	$Rank[$i]=$Rank[$i]/$factor;
	if ($Rank[$i]>$new_max){
		$new_max=$Rank[$i];
	}
   }
   for ($i=0;$i<count($Rank);$i++){
	$Rank[$i]=$PR[$i]*(1-$dOcur)+($new_max-$Rank[$i])*$dOcur;
   }
   $Rank = shellSort($Rank,$Sites);
   for ($i=count($Rank)-1;$i>=0;$i--){
      print "<a href='".$Sites[$i]."'>".$Sites[$i]."</a>\n";
      print "(Rank Occurrences: ".$Rank[$i].")<br><br>\n";
   }
   $end_time = getmicrotime();

   print "query executed in ".(substr($end_time-$start_time,0,5))." seconds.";
}
else
{
   print "<form method='post'> Keyword: 
          <input type='text' size='20' name='keyword'>\n";
   print "<input type='submit' value='Search'></form>\n";
   if (sizeof($array)>2){
   	print "<h2>This searcher is not working for more than two words yet!</h2>\n";
   }
}

print "</body></html>\n";

// Simple function for retrieving the current timestamp in microseconds:
function getmicrotime()
{
   list($usec, $sec) = explode(" ",microtime());
   return ((float)$usec + (float)$sec);
}
function shellSort($array,&$array2)
{
 if (!$length = count($array)) {
  return $array;
 }
 $k = 0;
 $gap[0] = (int)($length/2);
 while($gap[$k]>1){
  $k++;
  $gap[$k] = (int)($gap[$k-1]/2);
 }
 
 for ($i = 0; $i <= $k; $i++) {
  $step = $gap[$i];
  for ($j = $step; $j<$length; $j++) {
   $temp = $array[$j];
   $temp2 = $array2[$j];
   $p = $j-$step;
   while ($p >= 0 && $temp < $array[$p]) {
    $array[$p+$step] = $array[$p];
    $array2[$p+$step] = $array2[$p];
    $p = $p-$step;
   }
   $array[$p+$step] = $temp;
   $array2[$p+$step] = $temp2;
  }
 }
 return $array;
}

?>
