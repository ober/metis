(in-package :metis)

(defvar *db-backend* :manardb) ;; :sqlite :postgres

(defvar *mytasks* (list))

(defun have-we-seen-this-file (file)
  (let ((found (db-have-we-seen-this-file file)))
    (if found
	t
	nil)))

(defun walk-ct (path fn)
  (cl-fad:walk-directory path fn))

(defun sync-ct-file (x)
  (process-ct-file x))

(defun async-ct-file (x)
  (push (pcall:pexec
	  (funcall #'process-ct-file x)) *mytasks*))

(defun process-ct-file (x)
  "Handle the contents of the json gzip file"
  (when (equal (pathname-type x) "gz")
    (let ((*conn* (sqlite-emit-conn)))
      (declare (special *conn*))
      (unless (db-have-we-seen-this-file x)
	(progn
	  (db-mark-file-processed x)
	  (parse-ct-contents x))))))

(defun fetch-value (indicators plist)
  "Return the value at the end of the indicators list"
  (reduce #'getf indicators :initial-value plist))

(defun parse-ct-contents (x)
  "process the json output"
  (let* ((records (second (read-json-gzip-file x)))
	 (num (length records))
	 (btime (get-internal-real-time)))
    (dolist (x records)
      (normalize-insert (process-record x *fields*)))
    (let* ((etime (get-internal-real-time))
	   (delta (/ (float (- etime btime)) (float internal-time-units-per-second)))
	   (rps (ignore-errors (/ (float num) (float delta))))
	   (q-len (pcall-queue:queue-length to-db)))
      (format t "~%rps:~A rows:~A delta:~A q:~A" rps num delta q-len)
      (if (> q-len 100000)
	  (periodic-sync))
      (if (> num 100)
      (format t "~%rps:~A rows:~A delta:~A q:~A" rps num delta q-len)))))

(defun cloudtrail-report-sync (path)
  ;;(psql-begin)
  (force-output)
  ;;(sqlite-establish-connection)
  (let ((cloudtrail-reports (or path "~/CT")))
    (walk-ct cloudtrail-reports
	     #'sync-ct-file)))

(defun cloudtrail-report-async (workers path)
  ;;(psql-begin)
  (force-output)
  ;;(sqlite-establish-connection)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (let ((cloudtrail-reports (or path "~/CT")))
      (walk-ct cloudtrail-reports
	       #'async-ct-file))
    (mapc #'pcall:join *mytasks*)))
