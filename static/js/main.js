var url = '/_search';
var requestData = {};
var page = 1;
var persons = [];

$(document).ready(function () {

    new ClipboardJS('.btn_copy');

    let $grid = $('.grid').masonry({
        itemSelector: '.grid-item',
        fitWidth: true,
        columnWidth: 370,
        gutter: 5
    });

    $('.grid').click(function (e) {
        if ($(e.target).hasClass('grid-item')) {
            $(e.target).toggleClass('grid-item_gigante');
            $('.grid').masonry('layout');
        }
    });

    $('#searchField').val($.hashParam('q'));
    sendRequest(collectData());
});

function clear() {
    page = 1;
    let elements = $('div.grid div');
    $('.grid').masonry('remove', elements);
}

function collectData() {
    let data = {};
    data.person = [];
    data.page = page;
    data.q = $('#searchField').val();
    $('.person-block input:checked').each(function () {
        data.person.push($(this).attr('id'));
    });
    return data;
}

function sendRequest(data) {
    $.post(url, data, function (responseData) {
        inputData(responseData);
    });
}

let jPhotoBlock = $('#photoBlock');
jPhotoBlock.scroll(function () {
    if (jPhotoBlock.scrollTop() >= jPhotoBlock[0].scrollHeight - jPhotoBlock.height()) {
        page++;
        sendRequest(collectData());
    }
});

$('#searchField').on('keypress', function (e) {
    if (e.keyCode == 13) {
        persons.forEach(function (el) {
            $(document.getElementById(el)).prop('checked', false);
        });
        persons = [];
        clear();

        window.location.hash = "?q=" + $('#searchField').val();
        sendRequest(collectData());
    }
});

function inputData(data) {
    let allPersons = data.facets.person;
    let photo = data.hits;
    let jFacetBlock = $('#facetBlock .facets');

    $('#countPhoto').html('Всего: ' + data.count);

    jFacetBlock.html('');

    for (let i = 0; i < allPersons.length; i++) {
        let person = allPersons[i].toString().split(',');
        let htmlOutput = '<label><div class="person-block"><input type="checkbox" id="' + person[0] + '">' + person[0] + '<span>' + person[1] + '</span></div></label>';

        jFacetBlock.append(htmlOutput);
    }

    let html = [];
    for (let i = 0; i < photo.length; i++) {
        let path = photo[i].path.replace('\\thumbnails', '');
        html.push('<div class="grid-item"><img src="' + photo[i].path + '"><a class="btn_copy" data-clipboard-text="' + path + '" title="' + path + '">Copy link</a></div>');
    }

    var $items = $(html.join('\n'));
    $('.grid').append($items)
        .masonry('appended', $items)
        .imagesLoaded(function () {
            $('.grid').masonry('layout');
        });

    $('.person-block input').on('change', function () {
        $('#photoBlock').animate({ scrollTop: 0 }, 500, 'swing');
        if ($(this).is(':checked')) {
            persons.push($(this).attr('id'));
        } else {
            persons.splice($.inArray($(this).attr('id'), persons), 1);
        }
        clear();
        sendRequest(collectData());
    });

    persons.forEach(function (el) {
        $(document.getElementById(el)).prop('checked', true);
    });
}

$.hashParam = function (name) {
    var results = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.hash);
    if (results == null) {
        return null;
    }
    return decodeURI(results[1]) || 0;
}