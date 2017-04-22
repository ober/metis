(use files)
(use format)
(use list-bindings)
(use lmdb)
(use medea)
(use posix)
(use s11n)
(use srfi-1)
(use srfi-13)
(use srfi-19)
(use vector-lib)
(use z3)


(define *db* (lmdb-open (make-pathname "/home/ubuntu/" "metis.mdb") mapsize: 1000000000))
(lmdb-begin *db*)

(define myrecord (with-input-from-string (blob->string (lmdb-ref *db* (list-ref (lmdb-keys *db*) 888))) (cut deserialize)))


;; (define keys (lmdb-keys *db*))
;; (format #t "hi")
;; (define count (lmdb-count *db*))
;; (define key (lmdb-ref *db* (car keys)))
;; (format #t "key:~A~%" key)
;; (format #t "~A~%" (with-input-from-string
;; 		      (blob->string key)
;; 		    (lambda () (deserialize))))
(lmdb-end *db*)
(lmdb-close *db*)
