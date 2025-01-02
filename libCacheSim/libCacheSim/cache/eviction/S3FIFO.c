//
//  10% small FIFO + 90% main FIFO (2-bit Clock) + ghost
//  insert to small FIFO if not in the ghost, else insert to the main FIFO
//  evict from small FIFO:
//      if object in the small is accessed,
//          reinsert to main FIFO,
//      else
//          evict and insert to the ghost
//  evict from main FIFO:
//      if object in the main is accessed,
//          reinsert to main FIFO,
//      else
//          evict
//
//
//  S3FIFO.c
//  libCacheSim
//
//  Created by Juncheng on 12/4/22.
//  Copyright © 2018 Juncheng. All rights reserved.
//

#include "../../dataStructure/hashtable/hashtable.h"
#include "../../include/libCacheSim/evictionAlgo.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  cache_t *fifo;
  cache_t *fifo_ghost;
  cache_t *main_cache;
  cache_t *large_cache;  // LQ

  bool hit_on_ghost;

  int64_t n_obj_admit_to_fifo;
  int64_t n_obj_admit_to_main;
  int64_t n_obj_move_to_main;
  int64_t n_byte_admit_to_fifo;
  int64_t n_byte_admit_to_main;
  int64_t n_byte_move_to_main;

  int move_to_main_threshold;
  double fifo_size_ratio;
  double ghost_size_ratio;

  // DCP 
  double s_ratio;   // 小型 FIFO (S) 的佔比
  double m_ratio;   // 主 FIFO (M) 的佔比
  double lq_ratio;  // 大物件 LQ 的佔比

   // Weighted Eviction
  double alpha, beta, gamma;

  char main_cache_type[32];

  request_t *req_local;
} S3FIFO_params_t;

static const char *DEFAULT_CACHE_PARAMS =
    "s-ratio=0.10,m-ratio=0.70,lq-ratio=0.20,"
    "ghost-size-ratio=0.90,move-to-main-threshold=2,"
    "alpha=1.0,beta=0.5,gamma=2.0";

#define SOME_SCORE_THRESHOLD 5.0  // 根據實際需求調整

// ***********************************************************************
// ****                                                               ****
// ****                   function declarations                       ****
// ****                                                               ****
// ***********************************************************************
cache_t *S3FIFO_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params);
static void S3FIFO_free(cache_t *cache);
static bool S3FIFO_get(cache_t *cache, const request_t *req);

static cache_obj_t *S3FIFO_find(cache_t *cache, const request_t *req,
                                const bool update_cache);
static cache_obj_t *S3FIFO_insert(cache_t *cache, const request_t *req);
static cache_obj_t *S3FIFO_to_evict(cache_t *cache, const request_t *req);
static void S3FIFO_evict(cache_t *cache, const request_t *req);
static void S3FIFO_evict_lq(cache_t *cache, const request_t *req);
static bool S3FIFO_remove(cache_t *cache, const obj_id_t obj_id);
static inline int64_t S3FIFO_get_occupied_byte(const cache_t *cache);
static inline int64_t S3FIFO_get_n_obj(const cache_t *cache);
static inline bool S3FIFO_can_insert(cache_t *cache, const request_t *req);
static void S3FIFO_parse_params(cache_t *cache,
                                const char *cache_specific_params);

static void S3FIFO_evict_fifo(cache_t *cache, const request_t *req);
static void S3FIFO_evict_main(cache_t *cache, const request_t *req);
static double compute_weighted_score(cache_obj_t *obj, S3FIFO_params_t *params);

// ***********************************************************************
// ****                                                               ****
// ****                   end user facing functions                   ****
// ****                                                               ****
// ***********************************************************************

cache_t *S3FIFO_init(const common_cache_params_t ccache_params,
                     const char *cache_specific_params) {
  cache_t *cache =
      cache_struct_init("S3FIFO", ccache_params, cache_specific_params);
  cache->cache_init = S3FIFO_init;
  cache->cache_free = S3FIFO_free;
  cache->get = S3FIFO_get;
  cache->find = S3FIFO_find;
  cache->insert = S3FIFO_insert;
  cache->evict = S3FIFO_evict;
  cache->remove = S3FIFO_remove;
  cache->to_evict = S3FIFO_to_evict;
  cache->get_n_obj = S3FIFO_get_n_obj;
  cache->get_occupied_byte = S3FIFO_get_occupied_byte;
  cache->can_insert = S3FIFO_can_insert;

  cache->obj_md_size = 0;

  cache->eviction_params = malloc(sizeof(S3FIFO_params_t));
  memset(cache->eviction_params, 0, sizeof(S3FIFO_params_t));
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  params->req_local = new_request();
  params->hit_on_ghost = false;

  S3FIFO_parse_params(cache, DEFAULT_CACHE_PARAMS);
  if (cache_specific_params != NULL) {
    S3FIFO_parse_params(cache, cache_specific_params);
  }

  int64_t s_size  = (int64_t)(ccache_params.cache_size * params->s_ratio);
  int64_t m_size  = (int64_t)(ccache_params.cache_size * params->m_ratio);
  int64_t lq_size = (int64_t)(ccache_params.cache_size * params->lq_ratio);

  int64_t fifo_ghost_cache_size =
      (int64_t)(ccache_params.cache_size * params->ghost_size_ratio);

  common_cache_params_t ccache_params_local = ccache_params;
  // S
  ccache_params_local.cache_size = s_size;
  params->fifo = FIFO_init(ccache_params_local, NULL);

  if (fifo_ghost_cache_size > 0) {
    ccache_params_local.cache_size = fifo_ghost_cache_size;
    params->fifo_ghost = FIFO_init(ccache_params_local, NULL);
    snprintf(params->fifo_ghost->cache_name, CACHE_NAME_ARRAY_LEN,
             "FIFO-ghost");
  } else {
    params->fifo_ghost = NULL;
  }

  // M
  ccache_params_local.cache_size = m_size;
  params->main_cache = FIFO_init(ccache_params_local, NULL);

  // LQ (large object)
  ccache_params_local.cache_size = lq_size;
  params->large_cache = FIFO_init(ccache_params_local, NULL);
  snprintf(params->large_cache->cache_name, CACHE_NAME_ARRAY_LEN, "FIFO-LQ");

#if defined(TRACK_EVICTION_V_AGE)
  if (params->fifo_ghost != NULL) {
    params->fifo_ghost->track_eviction_age = false;
  }
  params->fifo->track_eviction_age = false;
  params->main_cache->track_eviction_age = false;
#endif

  snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "S3FIFO-%.4lf-%d",
           params->fifo_size_ratio, params->move_to_main_threshold);

  return cache;
}

/**
 * free resources used by this cache
 *
 * @param cache
 */
static void S3FIFO_free(cache_t *cache) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  free_request(params->req_local);
  params->fifo->cache_free(params->fifo);
  if (params->fifo_ghost != NULL) {
    params->fifo_ghost->cache_free(params->fifo_ghost);
  }
  params->main_cache->cache_free(params->main_cache);
  free(cache->eviction_params);
  cache_struct_free(cache);
}

/**
 * @brief this function is the user facing API
 * it performs the following logic
 *
 * ```
 * if obj in cache:
 *    update_metadata
 *    return true
 * else:
 *    if cache does not have enough space:
 *        evict until it has space to insert
 *    insert the object
 *    return false
 * ```
 *
 * @param cache
 * @param req
 * @return true if cache hit, false if cache miss
 */
static bool S3FIFO_get(cache_t *cache, const request_t *req) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  DEBUG_ASSERT(params->fifo->get_occupied_byte(params->fifo) +
                   params->main_cache->get_occupied_byte(params->main_cache) <=
               cache->cache_size);

  // 呼叫 DCP 檢查並調整
  DCP_check_and_adjust(cache);

  bool cache_hit = cache_get_base(cache, req);

  return cache_hit;
}

// ***********************************************************************
// ****                                                               ****
// ****       developer facing APIs (used by cache developer)         ****
// ****                                                               ****
// ***********************************************************************
/**
 * @brief find an object in the cache
 *
 * @param cache
 * @param req
 * @param update_cache whether to update the cache,
 *  if true, the object is promoted
 *  and if the object is expired, it is removed from the cache
 * @return the object or NULL if not found
 */
static cache_obj_t *S3FIFO_find(cache_t *cache, const request_t *req, const bool update_cache) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;

  // 如果不更新快取，只檢查 S, M, LQ
  if (!update_cache) {
    cache_obj_t *obj = params->fifo->find(params->fifo, req, false);
    if (obj != NULL) {
      return obj;
    }
    obj = params->main_cache->find(params->main_cache, req, false);
    if (obj != NULL) {
      return obj;
    }
    obj = params->large_cache->find(params->large_cache, req, false);
    if (obj != NULL) {
      return obj;
    }
    return NULL;
  }

  // 更新快取
  params->hit_on_ghost = false;
  cache_obj_t *obj = params->fifo->find(params->fifo, req, true);
  if (obj != NULL) {
    obj->S3FIFO.freq += 1;
    return obj;
  }

  if (params->fifo_ghost != NULL &&
      params->fifo_ghost->remove(params->fifo_ghost, req->obj_id)) {
    // 如果物件在 ghost 中，設置命中 ghost
    params->hit_on_ghost = true;
  }

  obj = params->main_cache->find(params->main_cache, req, true);
  if (obj != NULL) {
    obj->S3FIFO.freq += 1;
  }

  // 嘗試在 LQ 中查找
  obj = params->large_cache->find(params->large_cache, req, true);
  if (obj != NULL) {
    obj->S3FIFO.freq += 1;
  }

  return obj;
}

  /* update cache is true from now */
  params->hit_on_ghost = false;
  cache_obj_t *obj = params->fifo->find(params->fifo, req, true);
  if (obj != NULL) {
    obj->S3FIFO.freq += 1;
    return obj;
  }

  if (params->fifo_ghost != NULL &&
      params->fifo_ghost->remove(params->fifo_ghost, req->obj_id)) {
    // if object in fifo_ghost, remove will return true
    params->hit_on_ghost = true;
  }

  obj = params->main_cache->find(params->main_cache, req, true);
  if (obj != NULL) {
    obj->S3FIFO.freq += 1;
  }

  return obj;
}

/**
 * @brief insert an object into the cache,
 * update the hash table and cache metadata
 * this function assumes the cache has enough space
 * eviction should be
 * performed before calling this function
 *
 * @param cache
 * @param req
 * @return the inserted object
 */
static cache_obj_t *S3FIFO_insert(cache_t *cache, const request_t *req) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  cache_obj_t *obj = NULL;

  if (params->hit_on_ghost) {
    // insert into the main cache
    params->hit_on_ghost = false;
    obj = params->main_cache->insert(params->main_cache, req);
  } else {
    // 判斷是否為大物件
    if (req->obj_size > size_threshold) {  // 定義 size_threshold
      obj = params->large_cache->insert(params->large_cache, req);
    } else {
      // 小物件 => 放 S
      obj = params->fifo->insert(params->fifo, req);
    }
  }

  // 初始化 freq
  if (obj != NULL) {
    obj->S3FIFO.freq = 0;
  }

  return obj;
}

static double compute_weighted_score(cache_obj_t *obj, S3FIFO_params_t *params) {
  return params->alpha * obj->S3FIFO.freq
         - params->beta * log(obj->obj_size)
         + params->gamma * obj->reloadCost;
}

/**
 * @brief find the object to be evicted
 * this function does not actually evict the object or update metadata
 * not all eviction algorithms support this function
 * because the eviction logic cannot be decoupled from finding eviction
 * candidate, so use assert(false) if you cannot support this function
 *
 * @param cache the cache
 * @return the object to be evicted
 */
static cache_obj_t *S3FIFO_to_evict(cache_t *cache, const request_t *req) {
  assert(false);
  return NULL;
}

static void S3FIFO_evict_fifo(cache_t *cache, const request_t *req) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  cache_t *fifo = params->fifo;
  cache_t *ghost = params->fifo_ghost;
  cache_t *main = params->main_cache;

  bool has_evicted = false;
  while (!has_evicted && fifo->get_occupied_byte(fifo) > 0) {
    // 取出 FIFO 尾端物件
    cache_obj_t *obj_to_evict = fifo->to_evict(fifo, req);
    DEBUG_ASSERT(obj_to_evict != NULL);
    
    // 計算加權分數
    double score = params->alpha * obj_to_evict->S3FIFO.freq
                   - params->beta * log(obj_to_evict->obj_size)
                   + params->gamma * obj_to_evict->reloadCost;
    
    if (score >= SOME_SCORE_THRESHOLD) {
      // 分數高，移到 M
      main->insert(main, obj_to_evict->req_local);
    } else {
      // 分數低，踢到 ghost
      if (ghost != NULL) {
        ghost->get(ghost, obj_to_evict->req_local);
      }
    }
    
    // 從 FIFO 中移除物件
    bool removed = fifo->remove(fifo, obj_to_evict->obj_id);
    assert(removed);
    
    has_evicted = true;
  }
}

static void S3FIFO_evict_main(cache_t *cache, const request_t *req) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  cache_t *main = params->main_cache;
  cache_t *ghost = params->fifo_ghost;

  bool has_evicted = false;
  while (!has_evicted && main->get_occupied_byte(main) > main->cache_size) {
    // 取出主快取尾端物件
    cache_obj_t *obj_to_evict = main->to_evict(main, req);
    DEBUG_ASSERT(obj_to_evict != NULL);
    
    // 計算加權分數
    double score = params->alpha * obj_to_evict->S3FIFO.freq
                   - params->beta * log(obj_to_evict->obj_size)
                   + params->gamma * obj_to_evict->reloadCost;
    
    if (score >= SOME_SCORE_THRESHOLD) {
      // 分數高，保留在主快取，更新頻率
      obj_to_evict->S3FIFO.freq = MIN(obj_to_evict->S3FIFO.freq, 3) - 1;
      main->insert(main, obj_to_evict->req_local);
    } else {
      // 分數低，踢到 ghost
      if (ghost != NULL) {
        ghost->get(ghost, obj_to_evict->req_local);
      }
    }
    
    // 從主快取中移除物件
    bool removed = main->remove(main, obj_to_evict->obj_id);
    assert(removed);
    
    has_evicted = true;
  }
}

/**
 * @brief evict an object from the cache
 * it needs to call cache_evict_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param req not used
 * @param evicted_obj if not NULL, return the evicted object to caller
 */
static void S3FIFO_evict(cache_t *cache, const request_t *req) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;

  cache_t *fifo = params->fifo;
  cache_t *ghost = params->fifo_ghost;
  cache_t *main = params->main_cache;
  cache_t *large = params->large_cache;

  // 判斷 S, M, LQ 是否超量
  if (fifo->get_occupied_byte(fifo) > fifo->cache_size) {
    S3FIFO_evict_fifo(cache, req);
  } else if (main->get_occupied_byte(main) > main->cache_size) {
    S3FIFO_evict_main(cache, req);
  } else if (large->get_occupied_byte(large) > large->cache_size) {
    S3FIFO_evict_lq(cache, req);
  }
}

static void S3FIFO_evict_lq(cache_t *cache, const request_t *req) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  cache_t *large = params->large_cache;
  cache_t *main = params->main_cache;
  cache_t *ghost = params->fifo_ghost;

  bool has_evicted = false;
  while (!has_evicted && large->get_occupied_byte(large) > large->cache_size) {
    // 取出 LQ 尾端物件
    cache_obj_t *obj_to_evict = large->to_evict(large, req);
    DEBUG_ASSERT(obj_to_evict != NULL);
    
    // 計算加權分數
    double score = params->alpha * obj_to_evict->S3FIFO.freq
                   - params->beta * log(obj_to_evict->obj_size)
                   + params->gamma * obj_to_evict->reloadCost;
    
    if (score >= SOME_SCORE_THRESHOLD) {
      // 分數高，升級到 M
      main->insert(main, obj_to_evict->req_local);
    } else {
      // 分數低，踢到 ghost
      if (ghost != NULL) {
        ghost->get(ghost, obj_to_evict->req_local);
      }
    }
    
    // 移除 LQ 中的物件
    bool removed = large->remove(large, obj_to_evict->obj_id);
    assert(removed);
    
    has_evicted = true;
  }
}

/**
 * @brief remove an object from the cache
 * this is different from cache_evict because it is used to for user trigger
 * remove, and eviction is used by the cache to make space for new objects
 *
 * it needs to call cache_remove_obj_base before returning
 * which updates some metadata such as n_obj, occupied size, and hash table
 *
 * @param cache
 * @param obj_id
 * @return true if the object is removed, false if the object is not in the
 * cache
 */
static bool S3FIFO_remove(cache_t *cache, const obj_id_t obj_id) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  bool removed = false;
  removed = removed || params->fifo->remove(params->fifo, obj_id);
  removed = removed || (params->fifo_ghost &&
                        params->fifo_ghost->remove(params->fifo_ghost, obj_id));
  removed = removed || params->main_cache->remove(params->main_cache, obj_id);

  return removed;
}

static inline int64_t S3FIFO_get_occupied_byte(const cache_t *cache) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  return params->fifo->get_occupied_byte(params->fifo) +
         params->main_cache->get_occupied_byte(params->main_cache) +
         params->large_cache->get_occupied_byte(params->large_cache);
}
static inline int64_t S3FIFO_get_n_obj(const cache_t *cache) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;
  return params->fifo->get_n_obj(params->fifo) +
         params->main_cache->get_n_obj(params->main_cache);
}

static inline bool S3FIFO_can_insert(cache_t *cache, const request_t *req) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;

  if (req->obj_size > params->size_threshold) {
    return req->obj_size <= params->large_cache->cache_size;
  } else {
    return req->obj_size <= params->fifo->cache_size;
  }
}

// 回傳固定的大物件請求量
static double get_recent_large_requests(cache_t *cache) {
    return 100.0;
}

// 回傳固定的小物件請求量
static double get_recent_small_requests(cache_t *cache) {
    return 300.0;
}

static void DCP_check_and_adjust(cache_t *cache) {
    S3FIFO_params_t *params = (S3FIFO_params_t *)cache->eviction_params;

    // 使用固定值來模擬近期的大物件和小物件請求量
    double recent_large_requests = get_recent_large_requests(cache);
    double recent_small_requests = get_recent_small_requests(cache);
    
    // 定義調整的門檻值和步驟
    const double LARGE_REQUEST_THRESHOLD = 200.0;
    const double SMALL_REQUEST_THRESHOLD = 400.0;
    const double ADJUST_RATIO_STEP = 0.05;

    // 根據請求量調整比例
    if (recent_large_requests > LARGE_REQUEST_THRESHOLD) {
        // 增加 LQ 區域或給 M 更多空間
        params->lq_ratio += ADJUST_RATIO_STEP;
        params->s_ratio -= ADJUST_RATIO_STEP / 2;
        params->m_ratio -= ADJUST_RATIO_STEP / 2;
    } else if (recent_small_requests > SMALL_REQUEST_THRESHOLD) {
        // 減少 LQ，釋放空間給 S 和 M
        params->lq_ratio -= ADJUST_RATIO_STEP;
        params->s_ratio += ADJUST_RATIO_STEP / 2;
        params->m_ratio += ADJUST_RATIO_STEP / 2;
    }
    
    // 確保比例總和為 1
    normalize_ratios(params);

    // 更新快取區大小
    int64_t s_size  = (int64_t)(cache->cache_size * params->s_ratio);
    int64_t m_size  = (int64_t)(cache->cache_size * params->m_ratio);
    int64_t lq_size = (int64_t)(cache->cache_size * params->lq_ratio);
    
    // 更新各快取區域大小
    resize_cache(params->fifo, s_size);
    resize_cache(params->main_cache, m_size);
    resize_cache(params->large_cache, lq_size);
    
    // 呼叫剔除流程以適應新大小
    if (params->fifo->get_occupied_byte(params->fifo) > s_size) {
        S3FIFO_evict_fifo(cache, NULL);
    }
    if (params->main_cache->get_occupied_byte(params->main_cache) > m_size) {
        S3FIFO_evict_main(cache, NULL);
    }
    if (params->large_cache->get_occupied_byte(params->large_cache) > lq_size) {
        S3FIFO_evict_lq(cache, NULL);
    }
}


static void normalize_ratios(S3FIFO_params_t *params) {
  double total = params->s_ratio + params->m_ratio + params->lq_ratio;
  params->s_ratio /= total;
  params->m_ratio /= total;
  params->lq_ratio /= total;
}


// ***********************************************************************
// ****                                                               ****
// ****                parameter set up functions                     ****
// ****                                                               ****
// ***********************************************************************
static const char *S3FIFO_current_params(S3FIFO_params_t *params) {
  static __thread char params_str[128];
  snprintf(params_str, 128, "fifo-size-ratio=%.4lf,main-cache=%s\n",
           params->fifo_size_ratio, params->main_cache->cache_name);
  return params_str;
}

static void S3FIFO_parse_params(cache_t *cache,
                                const char *cache_specific_params) {
  S3FIFO_params_t *params = (S3FIFO_params_t *)(cache->eviction_params);

  char *params_str = strdup(cache_specific_params);
  char *old_params_str = params_str;
  char *end;

  while (params_str != NULL && params_str[0] != '\0') {
    /* different parameters are separated by comma,
     * key and value are separated by = */
    char *key = strsep((char **)&params_str, "=");
    char *value = strsep((char **)&params_str, ",");

    // skip the white space
    while (params_str != NULL && *params_str == ' ') {
      params_str++;
    }

    if (strcasecmp(key, "fifo-size-ratio") == 0) {
      params->fifo_size_ratio = strtod(value, NULL);
    } else if (strcasecmp(key, "ghost-size-ratio") == 0) {
      params->ghost_size_ratio = strtod(value, NULL);
    } else if (strcasecmp(key, "move-to-main-threshold") == 0) {
      params->move_to_main_threshold = atoi(value);
    } else if (strcasecmp(key, "s-ratio") == 0) {
      params->s_ratio = strtod(value, NULL);
    } else if (strcasecmp(key, "m-ratio") == 0) {
      params->m_ratio = strtod(value, NULL);
    } else if (strcasecmp(key, "lq-ratio") == 0) {
      params->lq_ratio = strtod(value, NULL);
    } else if (strcasecmp(key, "alpha") == 0) {
      params->alpha = strtod(value, NULL);
    } else if (strcasecmp(key, "beta") == 0) {
      params->beta = strtod(value, NULL);
    } else if (strcasecmp(key, "gamma") == 0) {
      params->gamma = strtod(value, NULL);
    } else if (strcasecmp(key, "print") == 0) {
      printf("parameters: %s\n", S3FIFO_current_params(params));
      exit(0);
    } else {
      ERROR("%s does not have parameter %s\n", cache->cache_name, key);
      exit(1);
    }
  }

  free(old_params_str);
}

#ifdef __cplusplus
}
#endif
