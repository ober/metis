(in-package :metis)


(defvar zs3::*credentials* (zs3:file-credentials "~/.aws/s3.conf"))
;;(defvar cl-user:*credentials* (zs3:file-credentials "~/.aws/s3.conf"))

(defvar *db-backend* :lmdb ) ;; :sqlite :postgres :lmdb :manardb

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
    (unless (db-have-we-seen-this-file x)
      (progn
	(db-mark-file-processed x)
	(parse-ct-contents x)))))

(defun fetch-value (indicators plist)
  "Return the value at the end of the indicators list"
  (reduce #'getf indicators :initial-value plist))

(defun get-filename-hash (file)
  (car (cl-ppcre:split #\.
		       (nth 4 (cl-ppcre:split "_" (file-namestring file))))))

(defun parse-ct-contents (x)
  "process the json output"
  (handler-case
      (progn
	(let* ((db nil)
	       (results '())
	       (records (second (read-json-gzip-file x)))
	       (num (length records))
	       (btime (get-internal-real-time)))

	    (dolist (x records)
	      (push (lmdb-normalize-insert (process-record x *fields*)) results))

	    (with-lmdb (db)
	    (mapc
	     #'(lambda (x)
		 (format t "x:~A~%" x))
		 results))
	  ;;`,(format t "with-lmdb: db:~A post:~A record:~A~%" ,db post record))))
	  (let* ((etime (get-internal-real-time))
		 (delta (/ (float (- etime btime)) (float internal-time-units-per-second)))
		 (rps (ignore-errors (/ (float num) (float delta)))))
	    (if (> num 100)
		(format t "~%rps:~A rows:~A delta:~A" rps num delta)))))
    (t (e) (error-print "parse-ct-contents" e))))

(defun cloudtrail-report-sync (path)
  ;;(setf *metis-need-files* t)
  ;;(if (boundp 'init-manardb) (init-manardb))
  ;;(init-ct-hashes)
  (force-output)
  (let ((cloudtrail-reports (or path "~/CT")))
    (walk-ct cloudtrail-reports
	     #'sync-ct-file)))


(defun cloudtrail-report-async (workers path)
  (setf *metis-need-files* t)
  (if (boundp 'init-manardb) (init-manardb))
  (init-ct-hashes)
  (force-output)
  (let ((workers (parse-integer workers)))
    (setf (pcall:thread-pool-size) workers)
    (let ((cloudtrail-reports (or path "~/CT")))
      (walk-ct cloudtrail-reports
	       #'async-ct-file))
    (mapc #'pcall:join *mytasks*)))

(defun sync-from-s3 (bucket)
  ;;(setf zs3::*credentials* (zs3:file-credentials "~/.aws/s3.conf"))
  (setf *credentials* (file-credentials "~/.aws/s3.conf"))
  (let
      ((exists (bucket-exists-p bucket)))
    (format t "bucket:~A exists:~A~%" bucket exists)
    (mapc #'(lambda (x)
	      (format t "~A~%"
		      (get-filename-hash
		       (slot-value x 'zs3:name))))
	  (coerce (all-keys bucket) 'list))))
