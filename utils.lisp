(in-package :metis)

(fare-memoization:define-memo-function get-hostname-by-ip (ip)
  ;; (unless (boundp '*benching*)
  ;;   (if (cl-ppcre:all-matches "^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$" ip)
  ;; 	(let ((name
  ;; 	       #+allegro
  ;; 		(ignore-errors (socket:ipaddr-to-hostname ip))
  ;; 		#+sbcl
  ;; 		(ignore-errors (sb-bsd-sockets:host-ent-name
  ;; 				(sb-bsd-sockets:get-host-by-address
  ;; 				 (sb-bsd-sockets:make-inet-address ip))))
  ;; 		#+lispworks
  ;; 		(ignore-errors (comm:get-host-entry ip :fields '(:name)))
  ;; 		#+clozure
  ;; 		(ignore-errors (ccl:ipaddr-to-hostname (ccl:dotted-to-ipaddr ip)))))
  ;; 	  (format t "ip:~A name:~A~%" ip name)
  ;; 	  (if (null name)
  ;; 	      ip
  ;; 	      name))
  ;; 	ip))
  "bogus.host.com")


#-clozure
(defun read-json-gzip-file (file)
  (with-input-from-string
      (s
       (uiop:run-program
	(format nil "zcat ~A" file)
	:output :string))
    (cl-json:decode-json s)))

#+clozure
(defun read-json-gzip-file (file)
  (with-input-from-string
      (s (apply #'concatenate 'string
		(gzip-stream:with-open-gzip-file (in file)
		  (loop for l = (read-line in nil nil)
		     while l collect l))))
    (cl-json:decode-json s)))

(defun cdr-assoc (item a-list &rest keys)
  (cdr (apply #'assoc item a-list keys)))

(define-setf-expander cdr-assoc (item a-list &rest keys)
  (let ((item-var (gensym))
        (a-list-var (gensym))
        (store-var (gensym)))
    (values
     (list item-var a-list-var)
     (list item a-list)
     (list store-var)
     `(let ((a-cons (assoc ,item-var ,a-list-var ,@ keys)))
        (if a-cons
            (setf (cdr a-cons) ,store-var)
            (setf ,a-list (acons ,item-var ,store-var ,a-list-var)))
        ,store-var)
     `(cdr (assoc ,item-var ,a-list-var ,@ keys)))))

(defun flatten (obj)
  (do* ((result (list obj))
        (node result))
       ((null node) (delete nil result))
    (cond ((consp (car node))
           (when (cdar node) (push (cdar node) (cdr node)))
           (setf (car node) (caar node)))
          (t (setf node (cdr node))))))

(defun exit ()
  #+allegro (excl:exit code)
  #+sbcl (sb-ext::exit)
  #+lispworks (lispworks:quit)
  #+clozure (ccl::quit)
  #+cmucl (quit)
  )

(defun file-string (path)
  (with-open-file (stream path)
    (let ((data (make-string (file-length stream))))
      (read-sequence data stream)
      data)))
