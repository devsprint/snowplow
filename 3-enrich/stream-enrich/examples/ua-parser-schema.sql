CREATE TABLE "atomic"."com_snowplowanalytics_snowplow_ua_parser_context_1" (
	"event_id" varchar(256) NOT NULL,
	"ts" timestamp NOT NULL,
	"useragent_family" varchar(256),
	"useragent_major" varchar(256),
	"useragent_minor" varchar(256),
	"useragent_patch" varchar(256),
	"useragent_version" varchar(256),
	"os_family" varchar(256),
	"os_major" varchar(64),
	"os_minor" varchar(64),
	"os_patch" varchar(64),
	"os_patch_minor" varchar(64),
	"os_version" varchar(64),
	"device_family" varchar(128)
)
DISTSTYLE KEY
DISTKEY (event_id)
SORTKEY (ts);