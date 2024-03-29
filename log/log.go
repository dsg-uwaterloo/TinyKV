// High level log wrapper, so it can output different log based on level.
//
// There are five levels in total: FATAL, ERROR, WARNING, INFO, DEBUG.
// The default log output level is INFO, you can change it by:
// - call log.SetLevel()
// - set environment variable `LOG_LEVEL`

package log

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
)

const (
	Ldate         = log.Ldate
	Llongfile     = log.Llongfile
	Lmicroseconds = log.Lmicroseconds
	Lshortfile    = log.Lshortfile
	LstdFlags     = log.LstdFlags
	Ltime         = log.Ltime
)

type (
	LogLevel int
	LogType  int
)

const (
	LOG_FATAL   = LogType(0x1)
	LOG_ERROR   = LogType(0x2)
	LOG_WARNING = LogType(0x4)
	LOG_INFO    = LogType(0x8)
	LOG_DEBUG   = LogType(0x10)
)

const (
	LOG_LEVEL_NONE  = LogLevel(0x0)
	LOG_LEVEL_FATAL = LOG_LEVEL_NONE | LogLevel(LOG_FATAL)
	LOG_LEVEL_ERROR = LOG_LEVEL_FATAL | LogLevel(LOG_ERROR)
	LOG_LEVEL_WARN  = LOG_LEVEL_ERROR | LogLevel(LOG_WARNING)
	LOG_LEVEL_INFO  = LOG_LEVEL_WARN | LogLevel(LOG_INFO)
	LOG_LEVEL_DEBUG = LOG_LEVEL_INFO | LogLevel(LOG_DEBUG)
	LOG_LEVEL_ALL   = LOG_LEVEL_DEBUG
)

const FORMAT_TIME_DAY string = "20060102"
const FORMAT_TIME_HOUR string = "2006010215"

var _log *Logger = New()

func init() {
	SetFlags(Ldate | Ltime | Lshortfile)
	SetHighlighting(runtime.GOOS != "windows")
}

func GlobalLogger() *log.Logger {
	return _log._log
}

func SetLevel(level LogLevel) {
	_log.SetLevel(level)
}
func GetLogLevel() LogLevel {
	return _log.level
}

func SetFlags(flags int) {
	_log._log.SetFlags(flags)
}

func Info(v ...interface{}) {
	_log.Info(v...)
}

func Infof(format string, v ...interface{}) {
	_log.Infof(format, v...)
}

func Panic(v ...interface{}) {
	_log.Panic(v...)
}

func Panicf(format string, v ...interface{}) {
	_log.Panicf(format, v...)
}

func Debug(v ...interface{}) {
	_log.Debug(v...)
}

func Debugf(format string, v ...interface{}) {
	_log.Debugf(format, v...)
}

func Warn(v ...interface{}) {
	_log.Warning(v...)
}

func Warnf(format string, v ...interface{}) {
	_log.Warningf(format, v...)
}

func Warning(v ...interface{}) {
	_log.Warning(v...)
}

func Warningf(format string, v ...interface{}) {
	_log.Warningf(format, v...)
}

func Error(v ...interface{}) {
	_log.Error(v...)
}

func Errorf(format string, v ...interface{}) {
	_log.Errorf(format, v...)
}

func Fatal(v ...interface{}) {
	_log.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	_log.Fatalf(format, v...)
}

func SetLevelByString(level string) {
	_log.SetLevelByString(level)
}

func SetHighlighting(highlighting bool) {
	_log.SetHighlighting(highlighting)
}

type Logger struct {
	_log         *log.Logger
	level        LogLevel
	highlighting bool
}

func (l *Logger) SetHighlighting(highlighting bool) {
	l.highlighting = highlighting
}

func (l *Logger) SetFlags(flags int) {
	l._log.SetFlags(flags)
}

func (l *Logger) Flags() int {
	return l._log.Flags()
}

func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

func (l *Logger) SetLevelByString(level string) {
	l.level = StringToLogLevel(level)
}

//func (l *Logger) log(t LogType, v ...interface{}) {
//	l.logf(t, "%v\n", v)
//}

func (l *Logger) logf(t LogType, format string, v ...interface{}) {
	if l.level|LogLevel(t) != l.level {
		return
	}

	logStr, logColor := LogTypeToString(t)
	var s string
	if l.highlighting {
		s = "\033" + logColor + "m[" + logStr + "] " + fmt.Sprintf(format, v...) + "\033[0m"
	} else {
		s = "[" + logStr + "] " + fmt.Sprintf(format, v...)
	}
	l._log.Output(4, s)
}

func (l *Logger) Fatal(v ...interface{}) {
	l.logf(LOG_FATAL, "%v\n", v...)
	os.Exit(-1)
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.logf(LOG_FATAL, format, v...)
	os.Exit(-1)
}

func (l *Logger) Panic(v ...interface{}) {
	l._log.Panic(v...)
}

func (l *Logger) Panicf(format string, v ...interface{}) {
	l._log.Panicf(format, v...)
}

func (l *Logger) Error(v ...interface{}) {
	l.logf(LOG_ERROR, "%v\n", v...)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logf(LOG_ERROR, format, v...)
}

func (l *Logger) Warning(v ...interface{}) {
	l.logf(LOG_WARNING, "%v\n", v...)
}

func (l *Logger) Warningf(format string, v ...interface{}) {
	l.logf(LOG_WARNING, format, v...)
}

func (l *Logger) Debug(v ...interface{}) {
	l.logf(LOG_DEBUG, "%v\n", v...)
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.logf(LOG_DEBUG, format, v...)
}

func (l *Logger) Info(v ...interface{}) {
	l.logf(LOG_INFO, "%v\n", v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.logf(LOG_INFO, format, v...)
}

func StringToLogLevel(level string) LogLevel {
	//如果为空，那么就取环境变量(否则，系统变量设置没用).
	if level == "" {
		level = os.Getenv("LOG_LEVEL")
	}
	switch level {
	case "fatal":
		return LOG_LEVEL_FATAL
	case "error":
		return LOG_LEVEL_ERROR
	case "warn":
		return LOG_LEVEL_WARN
	case "warning":
		return LOG_LEVEL_WARN
	case "debug":
		return LOG_LEVEL_DEBUG
	case "info":
		return LOG_LEVEL_INFO
	}
	//如果系统变量也是空，那么默认值取info(类似于从环境变量读取日志级别).
	return LOG_LEVEL_INFO
}

func LogTypeToString(t LogType) (string, string) {
	switch t {
	case LOG_FATAL:
		return "fatal", "[0;31"
	case LOG_ERROR:
		return "error", "[0;31"
	case LOG_WARNING:
		return "warning", "[0;33"
	case LOG_DEBUG:
		return "debug", "[0;36"
	case LOG_INFO:
		return "info", "[0;37"
	}
	return "unknown", "[0;37"
}

func New() *Logger {
	return NewLogger(os.Stderr, "")
}

func NewLogger(w io.Writer, prefix string) *Logger {
	var level LogLevel
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		level = StringToLogLevel(os.Getenv("LOG_LEVEL"))
	} else {
		level = LOG_LEVEL_INFO
	}
	return &Logger{_log: log.New(w, prefix, LstdFlags), level: level, highlighting: true}
}

func LogOutput(t LogType, format string, v ...interface{}) {
	LogOutputDepth(4, PT_none, t, format, v)
}

func LogOutputDepth(dep int, pkt PkgType, t LogType, format string, v ...interface{}) {
	l := _log
	if l.level|LogLevel(t) != l.level {
		return
	}

	logStr, logColor := LogTypeToString(t)
	var s string
	if l.highlighting {
		s = "\033" + logColor + "m[" + pkt.String() + logStr + "] " + fmt.Sprintf(format, v...) + "\033[0m"
	} else {
		s = "[" + pkt.String() + logStr + "] " + fmt.Sprintf(format, v...)
	}

	l._log.Output(dep, s)
}

type PkgType int

const (
	PT_none        = 0
	PT_raft        = 0x1
	PT_raftStore   = 0x2
	PT_raftStorage = 0x4
	//
	PT_testlog = 0x5
	//
	PT_test_txn = 0x6
	//
	PT_test_raftStore = 0x100
)

func (pt PkgType) String() string {
	switch pt {
	case PT_raft:
		return "<raft>"
	case PT_raftStore:
		return "<raft store>"
	case PT_raftStorage:
		return "<raft storage>"
	case PT_test_raftStore:
		return "<test raft-store>"
	case PT_test_txn:
		return "<test txn>"
	case PT_testlog:
		return "<test>"
	case PT_none:
		return "<none>"
	}
	return fmt.Sprintf("<unknow-%d>", pt)
}

var g_pt PkgType = 0

func AddPkgType(pkg PkgType) {
	g_pt |= pkg
}

func init() {
	AddPkgType(PT_test_txn)
}

func TxnDbg(format string, v ...interface{}) {
	PkgDebugf(PT_test_txn, 4, format, v...)
}

//this dep = 5;
func PkgDebugf(pkg PkgType, dep int, format string, v ...interface{}) {
	if g_pt&pkg != 0 {
		LogOutputDepth(dep, pkg, LOG_DEBUG, format, v...)
	}
}

func TestLog(format string, v ...interface{}) {
	LogOutputDepth(3, PT_testlog, LOG_ERROR, format, v...)
}
