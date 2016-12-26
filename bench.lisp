(in-package :metis)

(defun do-bench ()
  (setf *DB* "metis")
  ;;(declaim (optimize (safety 3) (speed 0) (debug 3)))
  (defvar *benching* t)
  ;;(defvar *db-backend* :manardb)
  ;;(cloudtrail-report-async "1" "~/nov/"))
  (cloudtrail-report-sync "~/nov/"))

(defun run-bench ()
  (sqlite-establish-connection)
  (defvar database "metis")
  (db-recreate-tables "metis")
  (princ "XXX: Running Test")
  ;;#+sbcl (time (do-bench))
  ;; (progn
  ;; 	(sb-sprof:with-profiling (:report :flat) (do-bench)))
  #+lispworks  ;;(hcl:extended-time (do-bench))
  (progn
    (hcl:set-up-profiler :package '(metis))
    (hcl:profile (do-bench)))
    ;;  #+allegro
    ;;(progn
    ;;(setf excl:*tenured-bytes-limit* 524288000)
    ;;(prof::with-profiling (:type :space) (ctcl::do-bench))
    ;;(prof::show-flat-profile))
  #+(or clozure abcl ecl) (time (do-bench))
    (time (do-bench))
    ;;(format t "results: size:~A" (queue-length *q*))
    ;;(cl-store:store *q* "~/q.store")

    )

;;(run-bench)
