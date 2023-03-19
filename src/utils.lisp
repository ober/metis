(in-package :metis)

(defun read-json-gzip-file (file)
  (handler-case
   (progn
     (let ((json (get-json-gzip-contents file)))
       (gethash "Records" (shasht:read-json json))
       ))
   (t (e) (error-print "read-json-gzip-file" e))))

(defun error-print (fn error)
  (format t "~%~A~%~A~%~A~%~A~%~A~%" fn fn error fn fn))

(defun get-json-gzip-contents (file)
  (uiop:run-program
   (format nil "zcat ~A" file)
   :output :string))

;; (defun get-json-gzip-contents (file)
;;   (handler-case
;;       (progn (first (gzip-stream:with-open-gzip-file (in file)
;; 		      (loop for l = (read-line in nil nil)
;; 			 while l collect l))))
;;     (t (e) (error-print "get-json-gzip-contents" e))))

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
  #+sbcl
  (make-hash-table :synchronized t :test 'equalp)
  #+ccl
  (make-hash-table :shared :lock-free :test 'equalp)
  #+(or allegro lispworks)
  (make-hash-table :test 'equalp))

(defun sort-uniq (list)
  (remove-duplicates (sort list #'string<=) :test #'equalp))

(defun rfc3339-to-epoch (datetime)
  (local-time:timestamp-to-unix
   (local-time:parse-timestring datetime)))
