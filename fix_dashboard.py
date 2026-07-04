with open('dashboard/app.py', 'r', encoding='utf-8') as f:
    content = f.read()

old = """        try:
            import pandas as _pd
            _drift = _pd.read_csv('data_drift.csv')
            if len(_drift) > 1:
                _drift['ts'] = _pd.to_datetime(_drift['timestamp'], errors='coerce')
                _fig_d = go.Figure()
                _fig_d.add_trace(go.Scatter(x=_drift['ts'], y=_drift['drift_score'], mode='lines+markers', line=dict(color=COLORS['brand_mid'], width=2.5), fill='tozeroy', fillcolor='rgba(139,92,246,0.1)'))
                _fig_d.add_hline(y=0.3, line_dash='dot', line_color=COLORS['negative'], annotation_text='Threshold 0.3')
                apply_chart_theme(_fig_d, height=280)
                st.plotly_chart(_fig_d, use_container_width=True)
            else:
                st.info('Accumulating drift data...')
        except Exception as _e:
            st.info('Drift detection warming up...')"""

new = """        try:
            import pandas as _pd
            _drift = _pd.read_csv('data_drift.csv')
            if len(_drift) > 1:
                _drift['ts'] = _pd.to_datetime(_drift['timestamp'], errors='coerce')
                _latest_score = float(_drift['drift_score'].iloc[-1])
                _col1, _col2 = st.columns([2, 1])
                with _col1:
                    _fig_d = go.Figure()
                    _fig_d.add_trace(go.Scatter(
                        x=_drift['ts'], y=_drift['drift_score'],
                        mode='lines+markers',
                        line=dict(color=COLORS['brand_mid'], width=2.5),
                        fill='tozeroy', fillcolor='rgba(139,92,246,0.1)',
                        name='Drift Score'
                    ))
                    _fig_d.add_hline(y=0.3, line_dash='dot',
                        line_color=COLORS['negative'],
                        annotation_text='Threshold 0.3',
                        annotation_font_color=COLORS['negative'])
                    apply_chart_theme(_fig_d, height=280, showlegend=True)
                    st.plotly_chart(_fig_d, use_container_width=True)
                with _col2:
                    _gauge = go.Figure(go.Indicator(
                        mode='gauge+number',
                        value=round(_latest_score, 3),
                        number=dict(font=dict(color=COLORS['text_primary'], size=28)),
                        gauge=dict(
                            axis=dict(range=[0, 1]),
                            bar=dict(color=COLORS['brand_mid'] if _latest_score < 0.3 else COLORS['negative'], thickness=0.3),
                            bgcolor='rgba(0,0,0,0)', borderwidth=0,
                            steps=[
                                dict(range=[0, 0.3], color='rgba(34,197,94,0.15)'),
                                dict(range=[0.3, 0.6], color='rgba(245,158,11,0.15)'),
                                dict(range=[0.6, 1], color='rgba(244,63,94,0.15)')
                            ]
                        )
                    ))
                    _gauge.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color=COLORS['text_secondary']),
                        margin=dict(l=10, r=10, t=30, b=10),
                        height=220
                    )
                    st.plotly_chart(_gauge, use_container_width=True)
                    _status = 'DRIFTING' if _latest_score > 0.3 else 'STABLE'
                    _color = COLORS['negative'] if _latest_score > 0.3 else COLORS['positive']
                    st.markdown(f'<div style="text-align:center;font-weight:700;color:{_color};font-size:1.1rem">{_status}</div>', unsafe_allow_html=True)
                _high = _drift[_drift['drift_score'] > 0.3].tail(3)
                if not _high.empty and 'before_titles' in _drift.columns:
                    st.markdown('<br>', unsafe_allow_html=True)
                    st.markdown(section_title('🔀', 'What Changed — Before vs After Drift'), unsafe_allow_html=True)
                    for _, _row in _high.iterrows():
                        _before = str(_row.get('before_titles', ''))[:150]
                        _after = str(_row.get('after_titles', ''))[:150]
                        _ts = str(_row['timestamp'])[:19]
                        _sc = _row['drift_score']
                        st.markdown(f'''<div class="pl-alert-card">
                            <b>🕐 {_ts}</b> &nbsp;·&nbsp; Score: <b>{_sc:.3f}</b><br><br>
                            <b style="color:{COLORS['neutral']}">BEFORE:</b> {_before}<br>
                            <b style="color:{COLORS['brand_end']}">AFTER &nbsp;:</b> {_after}
                        </div>''', unsafe_allow_html=True)
            else:
                st.info('Accumulating drift data — needs 2+ minute windows.')
        except Exception as _e:
            st.info(f'Drift detection warming up...')"""

if old in content:
    content = content.replace(old, new)
    with open('dashboard/app.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print('Successfully updated dashboard!')
else:
    print('Could not find the old code to replace.')