(in-package :metis)

(defun read-file-as-string (filename)
   (uiop:run-program
    (format nil "cat ~A" filename)
    :output :string))

(defun read-json-gzip-file (file)
  (handler-case
   (progn
     (let ((json (get-json-gzip-contents file)))
       ;;(jonathan:parse json)
       (gethash "Records" (shasht:read-json json))
       ))
   (t (e) (error-print "read-json-gzip-file" e))))

(defun error-print (fn error)
  (format t "~%~A~%~A~%~A~%~A~%~A~%" fn fn error fn fn))

(defun get-json-gzip-contents (file)
  (handler-case
      (progn (first (gzip-stream:with-open-gzip-file (in file)
		      (loop for l = (read-line in nil nil)
			 while l collect l))))
    (t (e) (error-print "get-json-gzip-contents" e))))

(defun cdr-assoc (item a-list &rest keys)
  (cdr (apply #'assoc item a-list keys)))

(defun flatten (obj)
    (cond ((atom obj) (list obj))
            ((null obj) nil)
            (t (append (flatten (car obj))
                       (flatten (cdr obj))))))

(defun file-string (path)
  (with-open-file (stream path)
    (let ((data (make-string (file-length stream))))
      (read-sequence data stream)
      data)))

(defun replace-all (string part replacement &key (test #'char=))
  "Returns a new string in which all the occurences of the part
is replaced with replacement."
  (with-output-to-string (out)
    (loop with part-length = (length part)
       for old-pos = 0 then (+ pos part-length)
       for pos = (search part string
			 :start2 old-pos
			 :test test)
       do (write-string string out
			:start old-pos
			:end (or pos (length string)))
       when pos do (write-string replacement out)
       while pos)))

(defun thread-safe-hash-table ()
  "Return A thread safe hash table"
  #+(or abcl ecl) (make-hash-table :test 'equalp)
  #+sbcl
  (make-hash-table :synchronized t :test 'equalp)
  #+ccl
  (make-hash-table :shared :lock-free :test 'equalp)
  #+(or allegro lispworks)
  (make-hash-table :test 'equalp))

(defun get-size (file)
  (let ((stat (get-stat file)))
    #+allegro (progn
		(require 'osi)
		(excl.osi:stat-size stat))
    #+lispworks (progn
		  (sys:file-stat-size stat))
    #+sbcl (sb-posix:stat-size stat)
    #+ccl (elt stat 2)
    ))

(defun hash-keys (hash-table)
  (loop for key being the hash-keys of hash-table collect key))
