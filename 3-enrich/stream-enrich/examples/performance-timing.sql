CREATE TABLE "atomic"."performance_timing" (
	"event_id" varchar(256) NOT NULL,
	"ts" timestamp NOT NULL,
	"navigationStart" int4(64),
	"unloadEventStart" int4(64),
	"unloadEventEnd" int4(64),
	"redirectStart" int4(64),
	"redirectEnd" int4(64),
	"fetchStart" int4(64),
	"domainLookupStart" int4(64),
	"domainLookupEnd" int4(64),
	"connectStart" int4(64),
	"connectEnd" int4(64),
	"secureConnectionStart" int4(64),
	"requestStart" int4(64),
	"responseStart" int4(64),
	"responseEnd" int4(64),
	"domLoading"  int4(64),
	"domInteractive" int4(64),
	"domContentLoadedEventStart" int4(64),
	"domContentLoadedEventEnd" int4(64),
  "domComplete" int4(64),
  "loadEventStart" int4(64),
  "loadEventEnd" int4(64),
  "chromeFirstPaint"  int4(64)
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (ts);