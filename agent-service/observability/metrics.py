def summarize(events, duration_ms):
    models = [event for event in events if event.get('type') == 'model_response']
    rag = [event for event in events if event.get('type') == 'rag']
    tools = [event for event in events if event.get('type') == 'tool_result']
    return {
        'duration_ms': round(duration_ms, 3),
        'model_calls': sum(event.get('type') == 'model_request' for event in events),
        'prompt_tokens': sum((event.get('usage') or {}).get('prompt_tokens', 0) for event in models),
        'completion_tokens': sum((event.get('usage') or {}).get('completion_tokens', 0) for event in models),
        'total_tokens': sum((event.get('usage') or {}).get('total_tokens', 0) for event in models),
        'usage_missing_calls': max(0, sum(event.get('type') == 'model_request' for event in events) - sum(bool(event.get('usage')) for event in models)),
        'embedding_tokens': sum((event.get('usage') or {}).get('total_tokens', 0) for event in rag),
        'tool_calls': len(tools),
        'http_requests': sum(len(event.get('requests', [])) for event in tools),
        'rag_calls': len(rag),
        'errors': sum(event.get('type') in {'error', 'model_error', 'degradation'} or
                      (event.get('type') == 'tool_result' and isinstance(event.get('result'), dict) and bool(event['result'].get('error'))) for event in events),
        'cost': {'currency_amount': None, 'reason': '未配置且未获取中转站实际计费；仅报告实际Token，不能据此虚构费用'},
    }
