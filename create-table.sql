-- Create table
create table t_tth
( 
  dt varchar2(100),
  ip varchar2(100),
  n varchar2(4000),
  s number,
  tth  varchar2(39)
);
create table t_sha1
(
  dt varchar2(100),
  ip varchar2(100),
  n varchar2(4000),
  s number,
  sha1  varchar2(40)
);
create table t_error
(
  dt varchar2(100),
  ip varchar2(100),
  cid varchar2(39),
  client varchar2(4000),
  dt_error varchar2(100),
  error  varchar2(4000)
);
