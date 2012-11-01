{-# LANGUAGE BangPatterns, DeriveDataTypeable, GeneralizedNewtypeDeriving #-}
-- |
-- Module:      Network.Riak.Resolvable.Internal
-- Copyright:   (c) 2011 MailRank, Inc.
-- License:     Apache
-- Maintainer:  Bryan O'Sullivan <bos@mailrank.com>
-- Stability:   experimental
-- Portability: portable
--
-- Storage and retrieval of data with automatic conflict resolution.
--
-- The 'put' and 'putMany' functions will attempt to perform automatic
-- conflict resolution a large number of times.  They will eventually
-- give up

module Network.Riak.Resolvable.Internal
    (
      Resolvable(..)
    , ResolvableMonoid(..)
    , get
    , getMany
    , getMerge
    , getWithLength
    , getWithLengthOpt
    , getMergeWithLength
    , modify
    , modify_
    , put
    , put_
    , putMany
    , putMany_
    ) where

import Control.Applicative ((<$>))
import Control.Arrow (first)
import Control.Exception (Exception, throwIO)
import Control.Monad (unless)
import Data.Aeson.Types (FromJSON, ToJSON)
import Data.Data (Data)
import Data.Either (partitionEithers)
import Data.Function (on)
import Data.List (foldl', sortBy)
import Data.Maybe (isJust, fromMaybe, fromJust)
import Data.Monoid (Monoid(mappend))
import Data.Typeable (Typeable)
import Network.Riak.Debug (debugValues)
import Network.Riak.Types.Internal hiding (MessageTag(..))


-- | A type that can automatically resolve a vector clock conflict
-- between two or more versions of a value.
--
-- Instances must be symmetric in their behaviour, such that the
-- following law is obeyed:
--
-- > resolve a b == resolve b a
--
-- Otherwise, there are no restrictions on the behaviour of 'resolve'.
-- The result may be @a@, @b@, a value derived from @a@ and @b@, or
-- something else.
--
-- If several conflicting siblings are found, 'resolve' will be
-- applied over all of them using a fold, to yield a single
-- \"winner\".
class (Show a) => Resolvable a where
    -- | Resolve a conflict between two values.
    resolve :: a -> a -> a

-- | A newtype wrapper that uses the 'mappend' method of a type's
-- 'Monoid' instance to perform vector clock conflict resolution.
newtype ResolvableMonoid a = RM { unRM :: a }
    deriving (Eq, Ord, Read, Show, Typeable, Data, Monoid, FromJSON, ToJSON)

instance (Eq a, Show a, Monoid a) => Resolvable (ResolvableMonoid a) where
    resolve = mappend
    {-# INLINE resolve #-}

instance (Resolvable a) => Resolvable (Maybe a) where
    resolve (Just a)   (Just b) = Just (resolve a b)
    resolve a@(Just _) _        = a
    resolve _          b        = b
    {-# INLINE resolve #-}

type Get a = Connection -> Bucket -> Key -> R -> IO (Maybe ([a], VClock))
type GetOpt a = Connection -> Bucket -> Key -> R -> Maybe Bool -> Maybe Bool -> IO (Maybe ([a], VClock))

get_ resolver doGet conn bucket key r =
  fmap (first resolver) `fmap` doGet conn bucket key r
{-# INLINE get_ #-}

getOpt_ resolver doGet conn bucket key r basic_quorum notfound_ok =
  fmap (first resolver) `fmap` doGet conn bucket key r basic_quorum notfound_ok
{-# INLINE getOpt_ #-}

get :: (Resolvable a) => Get a
    -> (Connection -> Bucket -> Key -> R -> IO (Maybe (a, VClock)))
get = get_ resolveMany
{-# INLINE get #-}

getWithLength :: (Resolvable a) => Get a
                 -> (Connection -> Bucket -> Key -> R -> IO (Maybe ((a, Int), VClock)))
getWithLength = get_ resolveManyWithLength
{-# INLINE getWithLength #-}

getWithLengthOpt :: (Resolvable a) => GetOpt a
                 -> (Connection -> Bucket -> Key -> R -> Maybe Bool -> Maybe Bool -> IO (Maybe ((a, Int), VClock)))
getWithLengthOpt = getOpt_ resolveManyWithLength
{-# INLINE getWithLengthOpt #-}

getMany :: (Resolvable a) =>
           (Connection -> Bucket -> [Key] -> R -> IO [Maybe ([a], VClock)])
        -> Connection -> Bucket -> [Key] -> R -> IO [Maybe (a, VClock)]
getMany doGet conn b ks r =
    map (fmap (first resolveMany)) `fmap` doGet conn b ks r
{-# INLINE getMany #-}

-- If Riak receives a put request with no vclock, and the given
-- bucket+key already exists, it will treat the missing vclock as
-- stale, ignore the put request, and send back whatever values it
-- currently knows about.  The same problem will arise if we send a
-- vclock that really is stale, but that's much less likely to occur.
-- We handle the missing-vclock case in the single-body-response case
-- of both put and putMany below, but we do not (can not?) handle the
-- stale-vclock case.

type Put a = Connection -> Bucket -> Key -> Maybe VClock -> a -> W -> DW
           -> IO ([a], VClock)

put :: (Resolvable a) => Put a
    -> Connection -> Bucket -> Key -> Maybe VClock -> a -> Maybe Int -> W -> DW
    -> IO (a, VClock)
put doPut conn bucket key mvclock0 val0 retries w dw = do
  let go !i val mvclock
         | i == fromMaybe maxRetries retries = return (val, fromJust mvclock)
         | otherwise       = do
        (xs, vclock) <- doPut conn bucket key mvclock val w dw
        case xs of
          [x] | i > 0 || isJust mvclock -> return (x, vclock)
          (_:_) -> do debugValues "put" "conflict" xs
                      go (i+1) (resolveMany' val xs) (Just vclock)
          []    -> unexError "Network.Riak.Resolvable" "put"
                   "received empty response from server"
  go (0::Int) val0 mvclock0
{-# INLINE put #-}

-- | The maximum number of times to retry conflict resolution.
maxRetries :: Int
maxRetries = 4
{-# INLINE maxRetries #-}

put_ :: (Resolvable a) =>
        (Connection -> Bucket -> Key -> Maybe VClock -> a -> W -> DW
                    -> IO ([a], VClock))
     -> Connection -> Bucket -> Key -> Maybe VClock -> a -> W -> DW
     -> IO ()
put_ doPut conn bucket key mvclock0 val0 w dw =
    put doPut conn bucket key mvclock0 val0 (Just maxRetries) w dw >> return ()
{-# INLINE put_ #-}

modify :: (Resolvable a) => Get a -> Put a
       -> Connection -> Bucket -> Key -> R -> W -> DW -> (Maybe a -> IO (a,b))
       -> IO (a,b)
modify doGet doPut conn bucket key r w dw act = do
  a0 <- get doGet conn bucket key r
  (a,b) <- act (fst <$> a0)
  (a',_) <- put doPut conn bucket key (snd <$> a0) a (Just maxRetries) w dw
  return (a',b)
{-# INLINE modify #-}

modify_ :: (Resolvable a) => Get a -> Put a
        -> Connection -> Bucket -> Key -> R -> W -> DW -> (Maybe a -> IO a)
        -> IO a
modify_ doGet doPut conn bucket key r w dw act = do
  a0 <- get doGet conn bucket key r
  a <- act (fst <$> a0)
  fst <$> put doPut conn bucket key (snd <$> a0) a (Just maxRetries) w dw
{-# INLINE modify_ #-}

getMerge :: (Resolvable a) => Get a -> Put a
        -> Connection -> Bucket -> Key -> Maybe Int -> R -> W -> DW
        -> IO (Maybe (a, VClock))
getMerge doGet doPut conn bucket key retries r w dw = do
  mg <- getWithLength doGet conn bucket key r
  case mg of
    Just ((v, l), vc) -> do
      if l > 1
        then do
          (v', vc') <- put doPut conn bucket key (Just vc) v retries w dw
          return $ Just (v', vc')
        else return $ Just (v, vc)
    Nothing -> return Nothing
{-# INLINE getMerge #-}

getMergeWithLength :: (Resolvable a) => Get a -> Put a
        -> Connection -> Bucket -> Key -> Maybe Int -> R -> W -> DW
        -> IO (Maybe ((a, Int), VClock))
getMergeWithLength doGet doPut conn bucket key retries r w dw = do
  mg <- getWithLength doGet conn bucket key r
  case mg of
    Just r@((v, l), vc) -> do
      if l > 1
        then do
          (v', vc') <- put doPut conn bucket key (Just vc) v retries w dw
          return $ Just ((v', l), vc')
        else return $ Just r
    Nothing -> return Nothing
{-# INLINE getMergeWithLength #-}

putMany :: (Resolvable a) =>
           (Connection -> Bucket -> [(Key, Maybe VClock, a)] -> W -> DW
                       -> IO [([a], VClock)])
        -> Connection -> Bucket -> [(Key, Maybe VClock, a)] -> W -> DW
        -> IO [(a, VClock)]
putMany doPut conn bucket puts0 w dw = go (0::Int) [] . zip [(0::Int)..] $ puts0
 where
  go _ acc [] = return . map snd . sortBy (compare `on` fst) $ acc
  go !i acc puts
      | i == maxRetries = go i (acc ++ (map unmush puts)) []
      | otherwise = do
    rs <- doPut conn bucket (map snd puts) w dw
    let (conflicts, ok) = partitionEithers $ zipWith mush puts rs
    unless (null conflicts) $
      debugValues "putMany" "conflicts" conflicts
    go (i+1) (ok++acc) conflicts
  mush (i,(k,mv,c)) (cs,v) =
      case cs of
        [x] | i > 0 || isJust mv -> Right (i,(x,v))
        (_:_) -> Left (i,(k,Just v, resolveMany' c cs))
        []    -> unexError "Network.Riak.Resolvable" "put"
                 "received empty response from server"
  unmush (i, (k, Just v, x)) = (i, (x, v))
{-# INLINE putMany #-}

putMany_ :: (Resolvable a) =>
            (Connection -> Bucket -> [(Key, Maybe VClock, a)] -> W -> DW
                        -> IO [([a], VClock)])
         -> Connection -> Bucket -> [(Key, Maybe VClock, a)] -> W -> DW -> IO ()
putMany_ doPut conn bucket puts0 w dw =
    putMany doPut conn bucket puts0 w dw >> return ()
{-# INLINE putMany_ #-}

resolveMany' :: (Resolvable a) => a -> [a] -> a
resolveMany' = foldl' resolve
{-# INLINE resolveMany' #-}

resolveMany :: (Resolvable a) => [a] -> a
resolveMany (a:as) = resolveMany' a as
resolveMany _      = error "resolveMany: empty list"
{-# INLINE resolveMany #-}

resolveManyWithLength :: (Resolvable a) => [a] -> (a, Int)
resolveManyWithLength l = (resolveMany l, length l)
{-# INLINE resolveManyWithLength #-}
